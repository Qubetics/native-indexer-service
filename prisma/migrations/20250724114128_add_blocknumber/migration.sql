/*
  Warnings:

  - You are about to drop the column `provAddress` on the `DvpnTransaction` table. All the data in the column will be lost.
  - The `price` column on the `DvpnTransaction` table would be dropped and recreated. This will lead to data loss if there is data in the column.

*/
-- AlterTable
ALTER TABLE "DvpnTransaction" DROP COLUMN "provAddress",
DROP COLUMN "price",
ADD COLUMN     "price" JSONB;
