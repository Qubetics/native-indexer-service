import { RedisClientType, createClient } from 'redis';
import { log } from '../Utility/common';
import { COLORS, REDIS_KEY } from '../constant';
import logger from '../lib/logger';

class RedisService {
  client: RedisClientType;

  static instance: RedisService;

  public static getInstance(): RedisService {
    if (!RedisService.instance) {
      RedisService.instance = new RedisService();
    }
    return RedisService.instance;
  }

  public async startRedis() {
    try {
      this.client = createClient({
        url: `redis://${environment.redisHost}:${environment.redisPort}`,
      });

      await this.client.connect();
      this.client.select(1);
      log('Redis connected ', COLORS.yellow, true, COLORS.green);

      this.client.on('connect', () => {
        logger.info('Connect to redis successfully. ');
      });
      this.client.on('ready', () => {
        logger.info('Ready');
      });
      this.client.on('reconnecting', () => {
        logger.info('Reconnecting');
      });

      this.client.on('end', () => {
        logger.info('End');
      });
      this.client.on('error', (err: Error) =>
        logger.error(
          '\x1b[31m%s\x1b[0m',
          'error while connecting redis server: ',
          err.message,
        ),
      );
    } catch (error) {
      console.log('error while connecting to redis .. ', error);
    }
  }

  // set the string data to redis
  public async setString<T>(key: string, data: T) {
    try {
      await this.client.set(key, JSON.stringify(data));
    } catch (err) {
      console.error('error while setting the data into redis: ', err);
    }
  }

  /**
   * save validator Data in set type of redis
   * @param key
   * @param data
   * @returns
   */
  public async upsertValidator<T>(key: string, id: number, data: any) {
    try {
      const datatoSave: {
        score: number;
        value: string;
      } = {
        score: 0,
        value: JSON.stringify(data),
      };

      const existingData =
        (await this.client.zRange(REDIS_KEY.VALIDATOR_DETAILS, 0, -1)).map(
          (item) => JSON.parse(item)
        ) || [];

      console.log('existingData : : ', existingData);

      if (existingData.length > 0) {
        const valExistingData = existingData.find(
          (i: any) =>
            // const extData = JSON.parse(i);
            i.validatorAddress === data.validatorAddress
        );
        console.log('data.validatorAddress : ', data.validatorAddress);
        console.log(
          'valExistingData?.validatorAddress : ',
          valExistingData?.validatorAddress,
        );

        // console.log('valExistingData : ', valExistingData);
        console.log('valExistingData : ', !!valExistingData);

        if (!valExistingData) {
          datatoSave.score = id;
          await this.client.zAdd(key, datatoSave);
          await this.setString(REDIS_KEY.VALIDATOR_ID_COUNTER, id + 1);
        } else {
          datatoSave.score = id - 1;
          await this.client.zRemRangeByScore(key, id - 1, id - 1);
          await this.client.zAdd(key, datatoSave);
        }
      } else {
        datatoSave.score = id;
        await this.client.zAdd(key, datatoSave);

        await this.setString(REDIS_KEY.VALIDATOR_ID_COUNTER, id + 1);
      }

      return true;
    } catch (err) {
      // console.log('error while setting the SET data into redis: ', err);
      return false;
    }
  }

  public async updateValidator(
    key: string,
    data: {
      score: string;
      value: string;
    }[]
  ) {
    try {
      const formattedData = data.map(({ score, value }) => [score, value]);
      console.log('formattedData   ', formattedData);

      // console.log('validator save res : ', saveRes);
    } catch (error) {
      console.log('error while saving : ', error);
    }
  }

  /**
   * save validator Data in set type of redis
   * @param key
   * @param data
   * @returns
   */
  public async upsertDelegator<T>(key: string, id: number, data: any) {
    try {
      const datatoSave: {
        score: number;
        value: string;
      } = {
        score: 0,
        value: JSON.stringify(data),
      };

      const existingData =
        (await this.client.zRangeByScore(key, id - 1, id - 1)) || [];

      if (existingData.length > 0) {
        const valExistingData = existingData.find((i: any) => {
          const extData = JSON.parse(i);
          // console.log(
          // 	extData.validatorAddress,
          // 	data.validatorAddress,
          // );
          // console.log(
          // 	extData.delegatorAddress,
          // 	data.delegatorAddress,
          // );

          return (
            extData.validatorAddress === data.validatorAddress &&
            extData.delegatorAddress === data.delegatorAddress
          );
        });

        if (!valExistingData) {
          datatoSave.score = id;
          await this.client.zAdd(key, datatoSave);
          await this.setString(REDIS_KEY.DELEGATOR_ID_COUNTER, id + 1);
        } else {
          datatoSave.score = id - 1;
          await this.client.zRemRangeByScore(key, id - 1, id - 1);
          await this.client.zAdd(key, datatoSave);
        }
      } else {
        datatoSave.score = id;
        await this.client.zAdd(key, datatoSave);
        await this.setString(REDIS_KEY.DELEGATOR_ID_COUNTER, id + 1);
      }

      // return res
    } catch (err) {
      // console.log('error while setting the SET data into redis: ', err);
    }
  }

  // get the string data from redis
  public async getString(key: string) {
    try {
      const res = (await this.client.get(key)) as string;
      return JSON.parse(res);
    } catch (err) {
      logger.error('error while getting the data from redis: ', err);
      return [];
    }
  }

  public async setHset(redisKey: string, dataKey: string, dataValue: object) {
    try {
      await this.client.hSet(redisKey, dataKey, JSON.stringify(dataValue));
      await this.client.expire(redisKey, environment.ttlTimeRewards * 60 * 60);
      console.log('ttlTimeRewards :', environment.ttlTimeRewards);
      return true;
    } catch (err) {
      logger.error('error while saving in redis: ', err);
      return null;
    }
  }

  public async getHset(redisKey: string, dataKey: string) {
    try {
      const res = (await this.client.hGet(redisKey, dataKey)) as string;
      return JSON.parse(res);
    } catch (err) {
      logger.error('error while saving in redis: ', err);
      return null;
    }
  }

  // get the string data from redis
  public async incrementValue<T>(key: string, value: number) {
    try {
      await this.client.incrByFloat(key, value);
      return true;
    } catch (err) {
      logger.error('Error while incrementing the data from redis: ', err);
      return false;
    }
  }
}
export default RedisService.getInstance();
