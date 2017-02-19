﻿using System;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.IO;
using System.Threading.Tasks;
using Sample.Images.FileStore;
using Sample.Images.Messages;
using SlimMessageBus;

namespace Sample.Images.Worker.Handlers
{
    public class GenerateThumbnailRequestHandler : IRequestHandler<GenerateThumbnailRequest, GenerateThumbnailResponse>
    {
        private readonly IFileStore _fileStore;
        private readonly IThumbnailFileIdStrategy _fileIdStrategy;

        public GenerateThumbnailRequestHandler(IFileStore fileStore, IThumbnailFileIdStrategy fileIdStrategy)
        {
            _fileStore = fileStore;
            _fileIdStrategy = fileIdStrategy;
        }

        #region Implementation of IRequestHandler<in GenerateThumbnailRequest,GenerateThumbnailResponse>

        public async Task<GenerateThumbnailResponse> OnHandle(GenerateThumbnailRequest request, string topic)
        {
            var image = await LoadImage(request.FileId);
            if (image == null)
            {
                throw new InvalidOperationException($"Image with id '{request.FileId}' does not exist");
            }
            using (image)
            {
                var thumbnailFileId = _fileIdStrategy.GetFileId(request.FileId, request.Width, request.Height, request.Mode);
                var thumbnail = ScaleToFitInside(image, request.Width, request.Height);

                using (thumbnail)
                {
                    SaveImage(thumbnailFileId, thumbnail);

                    return new GenerateThumbnailResponse
                    {
                        FileId = thumbnailFileId
                    };
                }                
            }
        }

        #endregion

        private async Task<Image> LoadImage(string fileId)
        {
            var imageContent = await _fileStore.GetFile(fileId);
            if (imageContent == null)
            {
                return null;
            }
            using (imageContent)
            {
                var image = Image.FromStream(imageContent);
                return image;
            }
        }

        private void SaveImage(string fileId, Image image)
        {
            using (var ms = new MemoryStream())
            {
                image.Save(ms, ImageFormat.Jpeg);
                ms.Seek(0, SeekOrigin.Begin);

                _fileStore.UploadFile(fileId, ms);
            }
        }

        private static Image ScaleToFitInside(Image imgPhoto, int targetW, int targetH)
        {
            // See https://www.codeproject.com/Articles/2941/Resizing-a-Photographic-image-with-GDI-for-NET

            var sourceX = 0;
            var sourceY = 0;
            var sourceWidth = imgPhoto.Width;
            var sourceHeight = imgPhoto.Height;

            var scaleW = targetW / (float) sourceWidth;
            var scaleH = targetH / (float) sourceHeight;
            var scale = Math.Min(scaleW, scaleH);

            var destX = 0;
            var destY = 0;
            var destWidth = (int)(sourceWidth * scale);
            var destHeight = (int)(sourceHeight * scale);

            var bmPhoto = new Bitmap(destWidth, destHeight, PixelFormat.Format24bppRgb);
            bmPhoto.SetResolution(imgPhoto.HorizontalResolution, imgPhoto.VerticalResolution);

            using (var grPhoto = Graphics.FromImage(bmPhoto))
            {
                grPhoto.InterpolationMode = InterpolationMode.HighQualityBicubic;

                grPhoto.DrawImage(imgPhoto,
                    new Rectangle(destX, destY, destWidth, destHeight),
                    new Rectangle(sourceX, sourceY, sourceWidth, sourceHeight),
                    GraphicsUnit.Pixel);
            }

            return bmPhoto;
        }
    }
}
