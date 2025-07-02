import ffmpeg from 'fluent-ffmpeg';
import { v2 as cloudinary } from 'cloudinary';
import dotenv from 'dotenv';
import Redis from 'ioredis';
import { Worker } from 'bullmq';
import mongoose from 'mongoose';
import Video from '../../media-api-service/models/Video.model.js';

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
console.log('Media Worker: Connected to MongoDB');

const mediaWorker = new Worker('media-jobs', async job => {
  const { videoId, fileUrl } = job.data;
  console.log(`Processing media for video ${videoId}: ${fileUrl}`);
  // Simulate media processing (transcode, thumbnail, etc.)
  await new Promise(res => setTimeout(res, 2000));
  // Optionally update video in DB (e.g., set processed flag)
  // await Video.findByIdAndUpdate(videoId, { $set: { mediaProcessed: true } });
  console.log(`Media processing complete for video ${videoId}`);
}, {
  connection: {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD
  }
});

mediaWorker.on('completed', job => {
  console.log(`Media job ${job.id} completed`);
});

mediaWorker.on('failed', (job, err) => {
  console.error(`Media job ${job?.id} failed:`, err);
});

// Process media job (video/audio)
export const processMedia = async (job) => {
  const { id, userId, username, originalName, mimetype, size, buffer, title, description, tags, processingOptions } = job.data;
  
  console.log(`🎥 Processing media: ${originalName} for user: ${username}`);
  
  try {
    // Update status to processing
    await updateJobStatus(job, 'processing', 10, 'Starting media processing...');
    
    // Determine media type
    const isVideo = mimetype.startsWith('video/');
    const isAudio = mimetype.startsWith('audio/');
    
    if (!isVideo && !isAudio) {
      throw new Error('Unsupported media type');
    }
    
    // Process media with FFmpeg
    const result = await processWithFFmpeg(buffer, processingOptions, job, isVideo);
    
    // Upload to Cloudinary
    await updateJobStatus(job, 'processing', 80, 'Uploading to cloud...');
    
    const uploadOptions = {
      resource_type: isVideo ? 'video' : 'audio',
      public_id: `media/${userId}/${id}`,
      overwrite: true,
      tags: tags
    };
    
    // Convert buffer to base64 for Cloudinary
    const base64File = result.buffer.toString('base64');
    const dataURI = `data:${mimetype};base64,${base64File}`;
    
    const uploadResult = await cloudinary.uploader.upload(dataURI, uploadOptions);
    
    // Generate thumbnail for video if requested
    let thumbnailUrl = null;
    if (isVideo && processingOptions.generateThumbnail && result.thumbnailBuffer) {
      const thumbnailBase64 = result.thumbnailBuffer.toString('base64');
      const thumbnailDataURI = `data:image/jpeg;base64,${thumbnailBase64}`;
      
      const thumbnailResult = await cloudinary.uploader.upload(thumbnailDataURI, {
        resource_type: 'image',
        public_id: `media/${userId}/${id}_thumb`,
        overwrite: true
      });
      
      thumbnailUrl = thumbnailResult.secure_url;
    }
    
    // Prepare final result
    const finalResult = {
      url: uploadResult.secure_url,
      thumbnail: thumbnailUrl,
      duration: result.duration,
      width: result.width,
      height: result.height,
      size: result.buffer.length,
      format: result.format,
      publicId: uploadResult.public_id
    };
    
    // Update status to completed
    await updateJobStatus(job, 'completed', 100, 'Media processing completed!', finalResult);
    
    console.log(`✅ Media processing completed: ${originalName}`);
    return finalResult;
    
  } catch (error) {
    console.error(`❌ Media processing failed: ${originalName}`, error);
    
    // Update status to failed
    await updateJobStatus(job, 'failed', 0, error.message);
    
    throw error;
  }
};

// Process media with FFmpeg
const processWithFFmpeg = (buffer, options, job, isVideo) => {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let thumbnailBuffer = null;
    let metadata = {};
    
    let command = ffmpeg()
      .input(buffer)
      .inputFormat('mp4'); // Adjust based on input format
    
    // Apply video processing options
    if (isVideo) {
      if (options.resize) {
        const { width, height } = options.resize;
        command = command.size(`${width}x${height}`);
      }
      
      if (options.quality) {
        command = command.videoBitrate(options.quality * 1000); // Convert to kbps
      }
      
      if (options.format) {
        command = command.outputFormat(options.format);
      }
    }
    
    // Apply audio processing options
    if (options.audioQuality) {
      command = command.audioBitrate(options.audioQuality * 1000);
    }
    
    // Generate thumbnail for video
    if (isVideo && options.generateThumbnail) {
      command = command
        .screenshots({
          timestamps: ['50%'],
          filename: 'thumbnail.jpg',
          folder: '/tmp'
        });
    }
    
    // Get metadata
    command = command.on('start', () => {
      updateJobStatus(job, 'processing', 20, 'FFmpeg processing started...');
    });
    
    command = command.on('progress', (progress) => {
      const percent = Math.min(70, 20 + (progress.percent || 0) * 0.5);
      updateJobStatus(job, 'processing', percent, `Processing: ${progress.percent?.toFixed(1)}%`);
    });
    
    command = command.on('end', () => {
      updateJobStatus(job, 'processing', 70, 'FFmpeg processing completed');
    });
    
    // Pipe to buffer
    command = command.pipe()
      .on('data', (chunk) => {
        chunks.push(chunk);
      })
      .on('end', () => {
        const processedBuffer = Buffer.concat(chunks);
        
        // Get metadata
        ffmpeg.ffprobe(processedBuffer, (err, metadata) => {
          if (err) {
            reject(err);
            return;
          }
          
          const result = {
            buffer: processedBuffer,
            thumbnailBuffer,
            duration: metadata.format.duration,
            width: metadata.streams[0]?.width,
            height: metadata.streams[0]?.height,
            format: metadata.format.format_name
          };
          
          resolve(result);
        });
      })
      .on('error', (err) => {
        reject(err);
      });
    
    command.run();
  });
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