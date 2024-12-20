import { PrismaClient } from '@prisma/client';
import { log } from '../Utility/common';
import { COLORS } from '../constant';
import logger from './logger';

const prisma = new PrismaClient();

async function connectToDatabase() {
	try {
		await prisma.$connect();
		log('Database Conneted ', COLORS.yellow, 'true', COLORS.green);
	} catch (error) {
		logger.error('Connection failed:', error);
	}
}

export { prisma, connectToDatabase };
