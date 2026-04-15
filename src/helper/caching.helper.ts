import { REDIS_KEY } from "../constant/index";
import { prisma } from "../lib/db";
import { RedisService } from "../service";

class ProcessHelper {
  async initializeCount() {
    const blockCount = await prisma.blocks.count();
    if (blockCount) {
      RedisService.setString(REDIS_KEY.NATIVE_BLOCKS_COUNT, blockCount);
    }
    const txCount = await prisma.transactions.count();
    if (txCount) {
      RedisService.setString(REDIS_KEY.NATIVE_TRANSACTIONS_COUNT, txCount);
    }
    const userCount = await prisma.users.count();
    if (userCount) {
      RedisService.setString(REDIS_KEY.NATIVE_USERS_COUNT, userCount);
    }
    const contractCount = await prisma.contracts.count();
    if (contractCount) {
      RedisService.setString(REDIS_KEY.NATIVE_CONTRACTS_COUNT, contractCount);
    }
  }
}
export default new ProcessHelper();
