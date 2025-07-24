/*
  Warnings:

  - You are about to drop the `Sessions` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropTable
DROP TABLE "Sessions";

-- CreateTable
CREATE TABLE "DvpnTransaction" (
    "id" SERIAL NOT NULL,
    "txhash" TEXT NOT NULL,
    "blocknumber" BIGINT NOT NULL,
    "planId" TEXT,
    "sessionId" TEXT,
    "subscriptionId" TEXT,
    "type" TEXT NOT NULL,
    "accAddress" TEXT,
    "provAddress" TEXT,
    "nodeAddress" TEXT,
    "downloadBytes" TEXT,
    "uploadBytes" TEXT,
    "maxBytes" TEXT,
    "duration" TEXT,
    "maxDuration" TEXT,
    "status" TEXT,
    "startAt" TEXT,
    "statusAt" TEXT,
    "inactiveAt" TEXT,
    "price" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DvpnTransaction_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "DvpnTransaction_txhash_key" ON "DvpnTransaction"("txhash");
