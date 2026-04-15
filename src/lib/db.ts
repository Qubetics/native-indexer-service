import { PrismaClient } from "@prisma/client";
import { log } from "../Utility/common";
import { COLORS } from "../constant";
import logger from "./logger";

const dbUrl = process.env.DATABASE_URL;
const urlWithPool = dbUrl?.includes("?")
  ? `${dbUrl}&connection_limit=25&pool_timeout=20`
  : `${dbUrl}?connection_limit=25&pool_timeout=20`;

const prisma = new PrismaClient({
  datasources: {
    db: {
      url: urlWithPool,
    },
  },
  log: ["error", "warn"],
});

async function connectToDatabase() {
  try {
    await prisma.$connect();
    log("Database Conneted ", COLORS.yellow, "true", COLORS.green);
  } catch (error) {
    logger.error("Connection failed:", error);
  }
}

export { prisma, connectToDatabase };
