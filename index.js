import { Worker } from 'bullmq';
import dotenv from 'dotenv';
import Redis from 'ioredis';

// Load environment variables
dotenv.config();

// Redis client
const redisClient = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: process.env.REDIS_PORT || 6379,
  password: process.env.REDIS_PASSWORD,
  retryDelayOnFailover: 100,
  maxRetriesPerRequest: null,
  lazyConnect: true
});

// Redis connection events
redisClient.on('connect', () => {
  console.log('✅ Redis connected');
});

redisClient.on('error', (err) => {
  console.error('❌ Redis error:', err);
});

redisClient.on('close', () => {
  console.log('🔌 Redis connection closed');
});

// Import processors
import { processImage } from './workers/imageProcessor.js';
import { processMedia } from './workers/mediaProcessor.js';

// Create workers
const imageWorker = new Worker('image-processing', processImage, {
  connection: redisClient,
  concurrency: 2
});

const mediaWorker = new Worker('media-processing', processMedia, {
  connection: redisClient,
  concurrency: 1
});

// Image worker event handlers
imageWorker.on('completed', (job) => {
  console.log(`✅ Image processing completed: ${job.id}`);
});

imageWorker.on('failed', (job, err) => {
  console.error(`❌ Image processing failed: ${job.id}`, err);
});

imageWorker.on('error', (err) => {
  console.error('❌ Image worker error:', err);
});

// Media worker event handlers
mediaWorker.on('completed', (job) => {
  console.log(`✅ Media processing completed: ${job.id}`);
});

mediaWorker.on('failed', (job, err) => {
  console.error(`❌ Media processing failed: ${job.id}`, err);
});

mediaWorker.on('error', (err) => {
  console.error('❌ Media worker error:', err);
});

console.log('🚀 Media workers started');
console.log('📸 Image processor: Ready');
console.log('🎥 Media processor: Ready');

// Graceful shutdown
const shutdown = async () => {
  console.log('🛑 Shutting down workers...');
  
  try {
    await imageWorker.close();
    await mediaWorker.close();
    await redisClient.disconnect();
    console.log('✅ Workers shut down gracefully');
    process.exit(0);
  } catch (error) {
    console.error('❌ Error during shutdown:', error);
    process.exit(1);
  }
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown); 