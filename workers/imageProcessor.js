import sharp from 'sharp';
import { v2 as cloudinary } from 'cloudinary';
import dotenv from 'dotenv';
import Redis from 'ioredis';
import { Worker } from 'bullmq';
import mongoose from 'mongoose';
import Post from '../../media-api-service/models/Post.model.js';

dotenv.config();

// Redis client
const redisClient = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: process.env.REDIS_PORT || 6379,
  password: process.env.REDIS_PASSWORD,
  retryDelayOnFailover: 100,
  maxRetriesPerRequest: 3,
  lazyConnect: true
});

// Configure Cloudinary
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET
});

// Connect to MongoDB
await mongoose.connect(process.env.MONGODB_URI);
console.log('Image Worker: Connected to MongoDB');

const imageWorker = new Worker('image-jobs', async job => {
  const { postId, imageUrl } = job.data;
  console.log(`Processing image for post ${postId}: ${imageUrl}`);
  // Simulate image processing (resize, optimize, etc.)
  await new Promise(res => setTimeout(res, 1000));
  // Optionally update post in DB (e.g., set processed flag)
  // await Post.findByIdAndUpdate(postId, { $set: { imageProcessed: true } });
  console.log(`Image processing complete for post ${postId}`);
}, {
  connection: {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD
  }
});

imageWorker.on('completed', job => {
  console.log(`Image job ${job.id} completed`);
});

imageWorker.on('failed', (job, err) => {
  console.error(`Image job ${job?.id} failed:`, err);
});

// Process image job
export const processImage = async (job) => {
  const { id, userId, username, originalName, mimetype, size, buffer, title, description, tags, processingOptions } = job.data;
  
  console.log(`📸 Processing image: ${originalName} for user: ${username}`);
  
  try {
    // Update status to processing
    await updateJobStatus(job, 'processing', 10, 'Starting image processing...');
    
    // Process image with Sharp
    let processedImage = sharp(buffer);
    
    // Apply resize if specified
    if (processingOptions.resize) {
      const { width, height } = processingOptions.resize;
      processedImage = processedImage.resize(width, height, {
        fit: 'inside',
        withoutEnlargement: true
      });
      await updateJobStatus(job, 'processing', 30, 'Resizing image...');
    }
    
    // Apply format conversion if specified
    if (processingOptions.format) {
      processedImage = processedImage.toFormat(processingOptions.format);
      await updateJobStatus(job, 'processing', 50, 'Converting format...');
    }
    
    // Apply quality settings if specified
    if (processingOptions.quality) {
      processedImage = processedImage.jpeg({ quality: processingOptions.quality });
      await updateJobStatus(job, 'processing', 70, 'Optimizing quality...');
    }
    
    // Get processed image buffer
    const processedBuffer = await processedImage.toBuffer();
    await updateJobStatus(job, 'processing', 80, 'Preparing for upload...');
    
    // Upload to Cloudinary
    const uploadOptions = {
      resource_type: 'image',
      public_id: `media/${userId}/${id}`,
      overwrite: true,
      tags: tags
    };
    
    // Convert buffer to base64 for Cloudinary
    const base64File = processedBuffer.toString('base64');
    const dataURI = `data:${mimetype};base64,${base64File}`;
    
    const uploadResult = await cloudinary.uploader.upload(dataURI, uploadOptions);
    
    await updateJobStatus(job, 'processing', 90, 'Uploading to cloud...');
    
    // Generate thumbnail if requested
    let thumbnailUrl = null;
    if (processingOptions.generateThumbnail) {
      const thumbnailBuffer = await sharp(buffer)
        .resize(300, 300, { fit: 'cover' })
        .jpeg({ quality: 80 })
        .toBuffer();
      
      const thumbnailBase64 = thumbnailBuffer.toString('base64');
      const thumbnailDataURI = `data:image/jpeg;base64,${thumbnailBase64}`;
      
      const thumbnailResult = await cloudinary.uploader.upload(thumbnailDataURI, {
        resource_type: 'image',
        public_id: `media/${userId}/${id}_thumb`,
        overwrite: true
      });
      
      thumbnailUrl = thumbnailResult.secure_url;
    }
    
    // Get image metadata
    const metadata = await sharp(processedBuffer).metadata();
    
    // Prepare result
    const result = {
      url: uploadResult.secure_url,
      thumbnail: thumbnailUrl,
      width: metadata.width,
      height: metadata.height,
      size: processedBuffer.length,
      format: metadata.format,
      publicId: uploadResult.public_id
    };
    
    // Update status to completed
    await updateJobStatus(job, 'completed', 100, 'Image processing completed!', result);
    
    console.log(`✅ Image processing completed: ${originalName}`);
    return result;
    
  } catch (error) {
    console.error(`❌ Image processing failed: ${originalName}`, error);
    
    // Update status to failed
    await updateJobStatus(job, 'failed', 0, error.message);
    
    throw error;
  }
};

// Update job status and publish to Redis
const updateJobStatus = async (job, status, progress, message, result = null) => {
  try {
    // Update job progress
    await job.updateProgress(progress);
    
    // Publish status update to Redis
    const statusUpdate = {
      type: 'processing-update',
      userId: job.data.userId,
      jobId: job.id,
      submissionId: job.data.id,
      status,
      progress,
      message,
      result,
      timestamp: new Date().toISOString()
    };
    
    await redisClient.publish(`processing:${job.id}`, JSON.stringify(statusUpdate));
    
  } catch (error) {
    console.error('❌ Error updating job status:', error);
  }
}; 