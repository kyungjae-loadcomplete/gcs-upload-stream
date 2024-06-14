#define GCP_UPLOAD_STREAM_NON_ASYNC

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using Google.Cloud.Storage.V1;

// GCS version of S3UploadStream by mlhpdx ( https://github.com/mlhpdx/s3-upload-stream/ )
namespace Utilities.GCP
{
    public class GcsUploadStream : Stream
    {
        const int PREFERRED_CHUNK_SIZE = 5 * 1024 * 1024; // all parts but the last this size or greater

        internal class Metadata
        {
            public Google.Apis.Storage.v1.Data.Object GcsObject = null;
            public int ChunkSize = PREFERRED_CHUNK_SIZE;
            public Uri UploadUri = null;
            public MemoryStream CurrentStream;
#if GCP_UPLOAD_STREAM_NON_ASYNC
            public byte[] CurrentBuffer;
#endif
            public long Position = 0; // based on bytes written
            public long Length = 0; // based on bytes written or SetLength, whichever is larger (no truncation)
            public long BytesSentToGcpSoFar = 0; // bytes request for uploading to GCS so far
        }

        Metadata _metadata = new Metadata();
        StorageClient _client = null;

        public GcsUploadStream(StorageClient client, string gsUri, string contentType = "application/octet-stream", long preferredChunkSize = PREFERRED_CHUNK_SIZE)
            : this(client, new Uri(gsUri), contentType, preferredChunkSize)
        {
        }

        public GcsUploadStream(StorageClient client, Uri gsUri, string contentType = "application/octet-stream", long preferredChunkSize = PREFERRED_CHUNK_SIZE)
            : this (client, gsUri.Host, gsUri.LocalPath.Substring(1), contentType, preferredChunkSize)
        {
        }

        public GcsUploadStream(StorageClient client, string bucket, string objectName, string contentType = "application/octet-stream", long preferredChunkSize = PREFERRED_CHUNK_SIZE)
            : this(client, new Google.Apis.Storage.v1.Data.Object
            {
                Bucket = bucket,
                Name = objectName,
                ContentType = contentType
            }, preferredChunkSize)
        {
        }

        public GcsUploadStream(StorageClient client, Google.Apis.Storage.v1.Data.Object gcsObject, long preferredChunkSize = PREFERRED_CHUNK_SIZE)
        {
            _client = client;
            _metadata.GcsObject = gcsObject;

            _metadata.ChunkSize = (int)(preferredChunkSize / UploadObjectOptions.MinimumChunkSize) * UploadObjectOptions.MinimumChunkSize;
            if (_metadata.ChunkSize != preferredChunkSize || _metadata.ChunkSize < UploadObjectOptions.MinimumChunkSize)
                _metadata.ChunkSize += UploadObjectOptions.MinimumChunkSize;
#if GCP_UPLOAD_STREAM_NON_ASYNC
            _metadata.CurrentBuffer = new byte[_metadata.ChunkSize];
#endif
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_metadata != null)
                {
                    Flush(true);
                    CompleteUpload();
                }
            }
            _metadata = null;
            base.Dispose(disposing);
        }
    
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => _metadata.Length = Math.Max(_metadata.Length, _metadata.Position);

        public override long Position
        {
            get => _metadata.Position;
            set => throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotImplementedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotImplementedException();

        public override void SetLength(long value)
        {
            _metadata.Length = Math.Max(_metadata.Length, value);
        }

        private void StartNewPart()
        {
            if (_metadata.CurrentStream != null) {
                Flush(false);
            }
#if GCP_UPLOAD_STREAM_NON_ASYNC
            _metadata.CurrentStream = new MemoryStream(_metadata.CurrentBuffer);
#else
            _metadata.CurrentStream = new MemoryStream();
#endif
        }

        public override void Flush()
        {
            Flush(false);
        }

        private void Flush(bool disposing)
        {
            if (disposing == false && (_metadata.CurrentStream == null || _metadata.CurrentStream.Position < _metadata.ChunkSize))
                return;

            if (_metadata.UploadUri == null)
            {
                _metadata.UploadUri = _client.InitiateUploadSessionAsync(_metadata.GcsObject,
                    null, new UploadObjectOptions()
                    {
                        ChunkSize = _metadata.ChunkSize
                    }).GetAwaiter().GetResult();
            }
            
            if (_metadata.CurrentStream != null)
            {
                var prevGcsPosition = _metadata.BytesSentToGcpSoFar;
#if GCP_UPLOAD_STREAM_NON_ASYNC
                _metadata.CurrentStream.SetLength(_metadata.CurrentStream.Position);
#endif
                var streamLength = _metadata.CurrentStream.Position;

                using var hc = new HttpClient();
                var messagesSent = new List<HttpRequestMessage>();

                long streamSendPosition = 0;
                while (streamSendPosition != streamLength - 1)
                {
                    _metadata.CurrentStream.Seek(streamSendPosition, SeekOrigin.Begin);

                    // make put request.
                    var request = new HttpRequestMessage(HttpMethod.Put, _metadata.UploadUri);
                    request.Content = new StreamContent(_metadata.CurrentStream);
                    request.Content.Headers.ContentLength = streamLength - streamSendPosition;
                    if (disposing)
                    {
                        request.Content.Headers.ContentRange = new ContentRangeHeaderValue(
                            prevGcsPosition + streamSendPosition,
                            prevGcsPosition + streamLength - 1,
                            prevGcsPosition + streamLength);
                    }
                    else
                    {
                        request.Content.Headers.ContentRange = new ContentRangeHeaderValue(
                            prevGcsPosition + streamSendPosition,
                            prevGcsPosition + streamLength - 1);
                    }

                    var response = hc.Send(request);

                    if ((int)response.StatusCode == 200 || (int)response.StatusCode == 201)
                        break; // finished!

                    if ((int)response.StatusCode == 308)
                    {
                        if (response.Headers.Contains("Range") == false)
                            throw new HttpRequestException("Range header not found in 308 response.");

                        var range = response.Headers.GetValues("Range").First();
                        var bytesGcpReceivedSoFar = long.Parse(range.Substring(range.LastIndexOf("-") + 1));
                        streamSendPosition = bytesGcpReceivedSoFar - prevGcsPosition;
                        continue;
                    }

                    throw new HttpRequestException(response.StatusCode.ToString());
                }

                _metadata.BytesSentToGcpSoFar += streamLength;

                foreach (var sentMessage in messagesSent)
                    sentMessage.Dispose();

                _metadata.CurrentStream.Dispose();
                _metadata.CurrentStream = null;
            }
        }

        private void CompleteUpload()
        {
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (count == 0) return;

            // write as much of the buffer as will fit to the current part, and if needed
            // allocate a new part and continue writing to it (and so on).
            var o = offset;
            var c = Math.Min(count, buffer.Length - offset); // don't over-read the buffer, even if asked to
            do
            {
                var streamWrittenByteCount = _metadata.CurrentStream?.Position ?? 0;
                if (_metadata.CurrentStream == null || streamWrittenByteCount == _metadata.ChunkSize)
                    StartNewPart();

                if (streamWrittenByteCount > _metadata.ChunkSize)
                    throw new OverflowException();

                var remaining = _metadata.ChunkSize - streamWrittenByteCount;
                var w = Math.Min(c, (int)remaining);
                _metadata.CurrentStream.Write(buffer, o, w);

                _metadata.Position += w;
                c -= w;
                o += w;
            } while (c > 0);
        }
    }
}
