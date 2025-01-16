import axios, { AxiosInstance } from 'axios';
import * as cron from 'cron';
import { QUEUE_NAME } from '../constant';
import ExlorerHelper from '../helper/db.helper';
import dbHelper from '../helper/db.helper';
import { getProposalsInfo } from '../helper/proposal';
import { calculateData, getValidatorsInfo } from '../helper/validator.helper';
import logger from '../lib/logger';
import rabbitmqService from '../service/rabbitmq.service';

const apiUrl = environment.evmHost;
let apiRes: AxiosInstance;

class CronService {
  constructor() {
    try {
      logger.info('cron service running...');
      apiRes = axios.create({
        baseURL: apiUrl,
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
          Accept: 'application/json',
        },
        timeout: 1000,
      });
    } catch (error) {
      console.log('error while making connection to rpc inside cron : ', error);
    }
  }

  public async saveTokenPrice() {
    async function fetchData() {
      await ExlorerHelper.saveTokenPriceByDate();
      setTimeout(fetchData, 60 * 60 * 1000); // Schedule the function to run again in 1 hour (60 minutes * 60 seconds * 1000 milliseconds)
    }

    await fetchData();
  }

  public async checkValidators() {
    async function fetchData() {
      await getValidatorsInfo(apiRes);
      await getProposalsInfo(apiRes);
      await calculateData();
      setTimeout(fetchData, 5 * 1000); // Change the interval to 2 seconds
    }
    // Call the function initially to start the scheduling
    await fetchData();
  }

  public async checkNullTimeStamp() {
    let isInProgress = false;
    new cron.CronJob(
      '*/10 * * * * *', // will run every 5 seconds.
      async () => {
        if (!isInProgress) {
          isInProgress = true;
          const nullTimestamp = await dbHelper.checkNullTimeStamp();
          if (nullTimestamp) {
            const randomNumber = Math.floor(Math.random() * 10) + 1;
            // eslint-disable-next-line no-restricted-syntax
            for (const r of nullTimestamp) {
              const txnHashQue = `${
                QUEUE_NAME.NULL_TIMESTAMP
              }_${+randomNumber}`;

              rabbitmqService.inQueueData(txnHashQue, JSON.stringify(r));
            }
            isInProgress = false;
          }
        }
      },
      null,
      true,
      'America/Los_Angeles'
    );
  }
}

export default new CronService();
