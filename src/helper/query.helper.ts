import { prisma } from '../lib/db';

export const getTxHourlyQuery = async (
  startDate: string,
  currentDate: Date,
) => prisma.$queryRaw`
    SELECT DATE_TRUNC('hour', "createdAt") AS "hour", COUNT(*)::int AS "count"
    FROM "Transactions"
    WHERE "createdAt" >= ${new Date(startDate)}
      AND "createdAt" <= ${currentDate}
      AND "type" = 'COIN_TRANSFER'
    GROUP BY "hour"
    ORDER BY "hour";
  `;

export const getContractTxHourlyQuery = async (
  startDate: Date,
  currentDate: Date,
) => prisma.$queryRaw`
    SELECT DATE_TRUNC('hour', "createdAt") AS "hour", COUNT(*)::int AS "count"
    FROM "Transactions"
    WHERE "createdAt" >= ${startDate}
      AND "createdAt" <= ${currentDate}
      AND "type" = 'CONTRACT_CREATION'
    GROUP BY "hour"
    ORDER BY "hour";
  `;

export const getTxDailyQuery = async (
  startDate: string,
  currentDate: Date,
) => prisma.$queryRaw`
    SELECT DATE_TRUNC('day', "createdAt") AS "day", COUNT(*)::int AS "count"
    FROM "Transactions"
    WHERE "createdAt" >= ${new Date(startDate)}
      AND "createdAt" <= ${currentDate}
      AND "type" = 'COIN_TRANSFER'
    GROUP BY "day"
    ORDER BY "day";
  `;

export const getContractTxDailyQuery = async (
  startDate: string,
  currentDate: Date,
) => prisma.$queryRaw`
    SELECT DATE_TRUNC('day', "createdAt") AS "day", COUNT(*)::int AS "count"
    FROM "Transactions"
    WHERE "createdAt" >= ${new Date(startDate)}
      AND "createdAt" <= ${currentDate}
      AND "type" = 'CONTRACT_CREATION'
    GROUP BY "day"
    ORDER BY "day";
  `;

export const getTxHourlyMinuteQuery = async (
  startDate: Date,
  currentDate: Date,
) => prisma.$queryRaw`
      SELECT DATE_TRUNC('minute', "createdAt") AS "hour", COUNT(*)::int AS "count"
      FROM "Transactions"
      WHERE "createdAt" >= ${startDate}
		AND "createdAt" <= ${currentDate}
        AND "type" = 'COIN_TRANSFER'
      GROUP BY "hour"
      ORDER BY "hour";
    `;

export const getContractTxHourlyMinuteQuery = async (
  startDate: Date,
  currentDate: Date,
) => prisma.$queryRaw`
      SELECT DATE_TRUNC('minute', "createdAt") AS "hour", COUNT(*)::int AS "count"
      FROM "Transactions"
      WHERE "createdAt" >= ${startDate}
		AND "createdAt" <= ${currentDate}
        AND "type" = 'CONTRACT_CREATION'
      GROUP BY "hour"
      ORDER BY "hour";
    `;
export const getAccountHourlyQuery = async (
  startDate: string,
  currentDate: Date,
) => prisma.$queryRaw`
    SELECT DATE_TRUNC('hour', "createdAt") AS "hour", COUNT(*)::int AS "count"
    FROM "Users"
    WHERE "createdAt" >= ${new Date(startDate)}
      AND "createdAt" <= ${currentDate}
   
    GROUP BY "hour"
    ORDER BY "hour";
  `;

export const getAccountDailyQuery = async (
  startDate: string,
  currentDate: Date,
) => prisma.$queryRaw`
    SELECT DATE_TRUNC('day', "createdAt") AS "day", COUNT(*)::int AS "count"
    FROM "Users"
    WHERE "createdAt" >= ${new Date(startDate)}
      AND "createdAt" <= ${currentDate}
      
    GROUP BY "day"
    ORDER BY "day";
  `;

export const getAccountHourlyMinuteQuery = async (
  startDate: Date,
  currentDate: Date,
) => prisma.$queryRaw`
      SELECT DATE_TRUNC('minute', "createdAt") AS "hour", COUNT(*)::int AS "count"
      FROM "Users"
      WHERE "createdAt" >= ${startDate}
		AND "createdAt" <= ${currentDate}
      
      GROUP BY "hour"
      ORDER BY "hour";
    `;

export const getTokenHourlyPriceQuery = async (
  startDate: string,
  currentDate: string,
) =>
  prisma.coinmarketInfo.findMany({
    where: {
      createdAt: {
        gte: new Date(startDate),
        lte: new Date(currentDate),
      },
    },
    orderBy: {
      createdAt: 'asc',
    },
    select: {
      createdAt: true,
      price: true,
    },
  });

export const getMinTxQuery = async (
  startDate: Date,
  currentDate: Date,
) => prisma.$queryRaw`
    SELECT DATE_TRUNC('minute', "createdAt") AS "hour", COUNT(*)::int AS "count"
				FROM "Transactions"
				WHERE "createdAt" >= ${new Date(startDate)}
				AND "createdAt" <= ${new Date(currentDate)}
				GROUP BY "hour"
				ORDER BY "hour";
  `;

export const getSecTxQuery = async (
  startDate: Date,
  currentDate: Date,
) => prisma.$queryRaw`
    	SELECT DATE_TRUNC('second', "createdAt") AS "second", COUNT(*)::int AS "count"
				FROM "Transactions"
				WHERE "createdAt" >= ${startDate}
					AND "createdAt" <= ${currentDate}
				GROUP BY "second"
				ORDER BY "second";
  `;

export const getHourlyTxQuery = async (
  startDate: string,
  currentDate: Date,
) => prisma.$queryRaw`
   SELECT DATE_TRUNC('hour', "createdAt") AS "hour", COUNT(*)::int AS "count"
			FROM "Transactions"
			WHERE "createdAt" >= ${new Date(startDate)}
			AND "createdAt" <= ${currentDate}
			GROUP BY "hour"
			ORDER BY "hour";
  `;

export const getTokenPriceGraph = async (startDate: Date, endDate: Date) =>
  prisma.coinmarketInfo.findMany({
    where: {
      createdAt: {
        gte: new Date(startDate),
        lte: new Date(endDate),
      },
    },
    orderBy: {
      createdAt: 'asc',
    },
    select: {
      createdAt: true,
      price: true,
    },
  });

export const getRowCount = async (tableName: string) => {
  try {
    const result: { row_count: string }[] =
      await prisma.$queryRaw`SELECT n_live_tup::bigint AS row_count FROM pg_stat_all_tables WHERE relname = ${tableName}`;

    const rowCount = result[0].row_count;
    return rowCount === '-1' ? 0 : Number(rowCount);
  } catch (error) {
    return 0;
  } finally {
    await prisma.$disconnect();
  }
};
