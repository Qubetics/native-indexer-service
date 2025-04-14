import {
  fillMissingDatesWithZeroByDay,
  fillMissingDatesWithZeroByhr,
  fillMissingMinutes,
  fillMissingSeconds,
  formatDate,
  formatDateByMin,
  formatDateBySec,
  formatDateForDay,
} from "../Utility/common";
import { REDIS_KEY, SOCKET_EVENT } from "../constant";
import { prisma } from "../lib/db";
import logger from "../lib/logger";
import SocketEventEmitter from "../service/socketEvents.helper";
import {
  getAccountDailyQuery,
  getAccountHourlyMinuteQuery,
  getAccountHourlyQuery,
  getContractTxDailyQuery,
  getContractTxHourlyMinuteQuery,
  getContractTxHourlyQuery,
  getHourlyTxQuery,
  getMinTxQuery,
  getSecTxQuery,
  getTxDailyQuery,
} from "./query.helper";

class SocketHelper {
  async emitSocket(key: string) {
    // if (key === REDIS_KEY.CONTRACT_DEPLOY_GRAPH) {
    //   await this.handleContractDeployGraphData(
    //     SOCKET_EVENT.CONTRACT_DEPLOYED_TX,
    //   );
    // } else if (key === REDIS_KEY.ACCOUNT_GRAPH) {
    //   await this.handleAccountData(SOCKET_EVENT.ACCOUNT_TX);
    // } else
    if (key === REDIS_KEY.TPS_GRAPH) {
      await this.handleAllTxData(SOCKET_EVENT.ALL_TRANSACTIONS);
    }
  }

  async handleAllTxData(socketEvent: string) {
    try {
      let graphData;
      let finalData;
      const allGraphData: {
        tenSecond: Array<{ createdAt: string; count: number }>;
        thirtyMinute: Array<{ createdAt: string; count: number }>;
        sixHour: Array<{ createdAt: string; count: number }>;
      } = {
        tenSecond: [],
        thirtyMinute: [],
        sixHour: [],
      };
      /** *******************minute******************************** */
      const currentDateByMinute = new Date();
      const currentDateModiOfMinute = formatDateByMin(new Date());
      const oneMinuteAgo = new Date();
      oneMinuteAgo.setMinutes(oneMinuteAgo.getMinutes() - 30);
      const startDateByMinute = formatDateByMin(oneMinuteAgo);

      graphData = (await getMinTxQuery(
        new Date(startDateByMinute),
        currentDateByMinute
      )) as Array<{
        hour: Date;
        count: number;
      }>;
      finalData = (await fillMissingMinutes(
        graphData,
        new Date(startDateByMinute),
        new Date(currentDateModiOfMinute)
      )) as Array<{ createdAt: string; count: number }>;

      allGraphData.thirtyMinute.push(...finalData);

      const currentDateByHour = new Date();

      const oneHourAgo = new Date();
      oneHourAgo.setHours(oneHourAgo.getHours() - 6);
      const startDateByHour = formatDate(oneHourAgo);

      graphData = (await getHourlyTxQuery(
        startDateByHour,
        currentDateByHour
      )) as Array<{ hour: Date; count: number }>;
      finalData = (await fillMissingDatesWithZeroByhr(
        graphData,
        new Date(startDateByHour)
      )) as Array<{ createdAt: string; count: number }>;
      allGraphData.sixHour.push(...finalData);

      const currentDateBySecond = new Date();
      const currentDateModi = formatDateBySec(new Date());
      const oneSecondAgo = new Date();
      oneSecondAgo.setSeconds(oneSecondAgo.getSeconds() - 9);
      const startDate = formatDateBySec(oneSecondAgo);

      graphData = (await getSecTxQuery(
        new Date(startDate),
        currentDateBySecond
      )) as Array<{
        second: Date;
        count: number;
      }>;
      finalData = (await fillMissingSeconds(
        graphData,
        new Date(startDate),
        new Date(currentDateModi)
      )) as Array<{ createdAt: string; count: number }>;
      allGraphData.tenSecond.push(...finalData);

      // const txCount = await RedisService.getString(REDIS_KEY.TOTAL_TX);
      // const counter = await ExlorerHelper.getTxCount();
      const obj = {
        finalData: allGraphData,
        count: await prisma.transactions.count(),
      };

      SocketEventEmitter.emitMessage(socketEvent, obj);
      return true;
    } catch (err) {
      logger.error("error occured while processing all tx");
      return false;
    }
  }

  async handleAccountData(socketEvent: string) {
    try {
      let graphData;
      let currentDate;

      let startDate;
      const allGraphData: {
        oneDay: Array<{ createdAt: string; count: number }>;
        sevenDay: Array<{ createdAt: string; count: number }>;
        thirtyDay: Array<{ createdAt: string; count: number }>;
        oneHour: Array<{ createdAt: string; count: number }>;
      } = {
        oneDay: [],
        sevenDay: [],
        thirtyDay: [],
        oneHour: [],
      };

      /** ************************************ */
      currentDate = new Date();
      const oneDayAgo = new Date();
      oneDayAgo.setDate(oneDayAgo.getDate() - 1); // Subtract 1 day
      startDate = formatDate(oneDayAgo);
      graphData = (await getAccountHourlyQuery(
        startDate,
        currentDate
      )) as Array<{ hour: Date; count: number }>;

      const finalDataByOneDay = (await fillMissingDatesWithZeroByhr(
        graphData,
        new Date(startDate)
      )) as Array<{ createdAt: string; count: number }>;
      allGraphData.oneDay.push(...finalDataByOneDay);

      /** ************************************ */

      const finalDataBySevenDay = await this.getDayData(7, "account");
      allGraphData.sevenDay.push(...finalDataBySevenDay);

      /** **************************************** */

      const finalDataByThirtyDay = await this.getDayData(30, "account");

      allGraphData.thirtyDay.push(...finalDataByThirtyDay);

      /** ************************************ */

      currentDate = new Date();
      const currentDateModi = formatDateByMin(new Date());
      const oneHourAgo = new Date();
      oneHourAgo.setHours(oneHourAgo.getHours() - 1);
      startDate = formatDateByMin(oneHourAgo);
      graphData = (await getAccountHourlyMinuteQuery(
        new Date(startDate),
        currentDate
      )) as Array<{ hour: Date; count: number }>;

      const finalDataOneHour = (await fillMissingMinutes(
        graphData,
        new Date(startDate),
        new Date(currentDateModi)
      )) as Array<{ createdAt: string; count: number }>;
      allGraphData.oneHour.push(...finalDataOneHour);

      /** ************************************ */

      // const accountCount = await RedisService.getString(
      // 	REDIS_KEY.TOTAL_ACCOUNT,
      // );

      // const counter = await ExlorerHelper.getUserCount();

      const obj = {
        finalData: allGraphData,
        // count: counter.data.count,
        count: await prisma.users.count(),
      };

      SocketEventEmitter.emitMessage(socketEvent, obj);
      return true;
    } catch (err) {
      logger.error(
        "error occured while handle Account Data , coin and contract deployed tx"
      );
      return false;
    }
  }

  async getDayData(period: number, type: string) {
    let graphData: Array<{ day: Date; count: number }> = [];
    const currentDate = new Date();
    const fromDateSeven = new Date(currentDate);
    fromDateSeven.setDate(fromDateSeven.getDate() - period);
    const startDate = formatDateForDay(fromDateSeven);
    if (type === "account") {
      graphData = (await getAccountDailyQuery(
        startDate,
        currentDate
      )) as Array<{ day: Date; count: number }>;
    } else if (type === "contract") {
      graphData = (await getContractTxDailyQuery(
        startDate,
        currentDate
      )) as Array<{ day: Date; count: number }>;
    } else if (type === "TPS") {
      graphData = (await getTxDailyQuery(startDate, currentDate)) as Array<{
        day: Date;
        count: number;
      }>;
    }
    const finalDataBySevenDay = await fillMissingDatesWithZeroByDay(
      graphData,
      new Date(startDate),
      currentDate
    );

    return finalDataBySevenDay;
  }

  async handleContractDeployGraphData(socketEvent: string) {
    try {
      /** **************************************** */

      let graphData;
      let currentDate;
      let startDate;
      const allGraphData: {
        oneDay: Array<{ createdAt: string; count: number }>;
        sevenDay: Array<{ createdAt: string; count: number }>;
        thirtyDay: Array<{ createdAt: string; count: number }>;
        oneHour: Array<{ createdAt: string; count: number }>;
      } = {
        oneDay: [],
        sevenDay: [],
        thirtyDay: [],
        oneHour: [],
      };

      currentDate = new Date();
      const oneDayAgo = new Date();
      oneDayAgo.setDate(oneDayAgo.getDate() - 1); // Subtract 1 day
      startDate = formatDate(oneDayAgo);
      graphData = (await getContractTxHourlyQuery(
        new Date(startDate),
        currentDate
      )) as Array<{ hour: Date; count: number }>;

      const finalDataByOneDay = (await fillMissingDatesWithZeroByhr(
        graphData,
        new Date(startDate)
      )) as Array<{ createdAt: string; count: number }>;

      allGraphData.oneDay.push(...finalDataByOneDay);

      /** **************************************** */

      const finalDataBySevenDay = await this.getDayData(7, "contract");

      allGraphData.sevenDay.push(...finalDataBySevenDay);

      /** **************************************** */

      const finalDataByThirtyDay = await this.getDayData(30, "contract");

      allGraphData.thirtyDay.push(...finalDataByThirtyDay);

      /** **************************************** */

      currentDate = new Date();
      const currentDateModi = formatDateByMin(new Date());
      const oneHourAgo = new Date();
      oneHourAgo.setHours(oneHourAgo.getHours() - 1);
      startDate = formatDateByMin(oneHourAgo);

      graphData = (await getContractTxHourlyMinuteQuery(
        new Date(startDate),
        currentDate
      )) as Array<{ hour: Date; count: number }>;
      const finalDataOneHour = (await fillMissingMinutes(
        graphData,
        new Date(startDate),
        new Date(currentDateModi)
      )) as Array<{ createdAt: string; count: number }>;
      allGraphData.oneHour.push(...finalDataOneHour);
      // const txCount = await RedisService.getString(
      // 	type === TRANSACTION_TYPE.COIN_TRANSFER
      // 		? REDIS_KEY.TOTAL_COIN_TRANSFER_TX
      // 		: REDIS_KEY.TOTAL_CONTRACT_DEPLOYED_TX,
      // );
      // const counter = await ExlorerHelper.getContractDeloyedTxCount();

      const obj = {
        finalData: allGraphData,
        count: await prisma.contracts.count(),
      };

      SocketEventEmitter.emitMessage(socketEvent, obj);
      return true;
    } catch (err) {
      logger.error(
        "Error occured while handle Graph Data, coin and contract deployed tx"
      );
      return false;
    }
  }
}
export default new SocketHelper();
