-- AlterTable
ALTER TABLE "Transactions" ALTER COLUMN "fromAddress" DROP NOT NULL,
ALTER COLUMN "toAddress" DROP NOT NULL;
