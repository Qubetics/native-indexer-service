/*
  Warnings:

  - The `startAt` column on the `DvpnTransaction` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `inactiveAt` column on the `DvpnTransaction` table would be dropped and recreated. This will lead to data loss if there is data in the column.

*/
-- AlterTable
ALTER TABLE "DvpnTransaction" DROP COLUMN "startAt",
ADD COLUMN     "startAt" TIMESTAMP(3),
DROP COLUMN "inactiveAt",
ADD COLUMN     "inactiveAt" TIMESTAMP(3);
