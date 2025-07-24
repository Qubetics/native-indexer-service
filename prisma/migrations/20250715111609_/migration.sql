-- CreateTable
CREATE TABLE "Sessions" (
    "id" TEXT NOT NULL,
    "subscriptionId" TEXT NOT NULL,
    "accAddress" TEXT NOT NULL,
    "nodeAddress" TEXT NOT NULL,
    "downloadBytes" TEXT NOT NULL,
    "uploadBytes" TEXT NOT NULL,
    "maxBytes" TEXT NOT NULL,
    "duration" TEXT NOT NULL,
    "maxDuration" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "startAt" TEXT NOT NULL,
    "statusAt" TEXT NOT NULL,
    "inactiveAt" TEXT NOT NULL,
    "price" JSONB,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Sessions_pkey" PRIMARY KEY ("id")
);
