import axios, { AxiosInstance, AxiosResponse } from 'axios';
import { bech32 } from 'bech32';
import { noExponent } from '../Utility/common';
import { RES_MSG, CONST_NAME, REDIS_KEY } from '../constant';
import {
  Params,
  // IValidator,
  ValidatorDelegations,
  IValidatorStake,
  IValidatorToSave,
} from '../interface/index';
import { RedisService } from '../service';
// import redisHelper from '../service/redis.helper';
import dbHelper from './db.helper';

async function getParams(apiRes: AxiosInstance): Promise<Params | false> {
  try {
    const [
      validatorResponse,
      circulationSupplyResponse,
      inflationRateResponse,
      poolResponse,
      paramsResponse,
      total_Supply,
      annualProvision,
    ] = await Promise.all([
      apiRes.get('/cosmos/staking/v1beta1/validators'),
      apiRes.get(
        `/cosmos/bank/v1beta1/supply/by_denom?denom=${environment.symbol.toLowerCase()}`,
      ),
      apiRes.get('/cosmos/mint/v1beta1/inflation'),
      apiRes.get('/cosmos/staking/v1beta1/pool'),
      apiRes.get('/cosmos/distribution/v1beta1/params'),
      apiRes.get('/cosmos/bank/v1beta1/supply'),
      apiRes.get('/cosmos/mint/v1beta1/annual_provisions'),
    ]);

    // Filter active validators
    const activeValidator = validatorResponse?.data?.validators.filter(
      (item: any) => item.status === CONST_NAME.BOND_STATUS,
    );
    if (activeValidator?.length === 0) {
      return false;
    }
    const totalValidator: number | undefined =
      validatorResponse?.data?.validators?.length;
    if (totalValidator === undefined) {
      return false;
    }
    const circulation_supply: number =
      Number(circulationSupplyResponse.data.amount.amount) / CONST_NAME.ETH_EXP;

    const inflation_rate = Number(inflationRateResponse.data.inflation);

    const formattedInflationRate = `${inflation_rate}`;

    let blocksPerYear = Number(CONST_NAME.BLOCK_PER_YEAR);
    blocksPerYear = Number(blocksPerYear / 2);
    const inflation = formattedInflationRate;

    const bondedTokens = Number(poolResponse.data.pool.bonded_tokens);

    const annualProvisions = annualProvision.data.annual_provisions;

    const totalSupply = total_Supply.data.supply[0].amount;

    const bondedRate = Number(((bondedTokens / totalSupply) * 100).toFixed(3));
    const communityTax = Number(paramsResponse.data.params.community_tax);

    return {
      annualProvisions,
      blocksPerYear,
      inflation,
      bondedTokens,
      communityTax,
      activeValidator: activeValidator?.length,
      totalValidator,
      circulation_supply,
      bondedRate,
      totalSupply,
    };
  } catch (error) {
    console.log('error occurred while calculating stake APR', error);
    return false;
  }
}

/**
 * retrive delegator reward amount
 * @param apiRes
 * @param address
 * @returns
 */
async function getDelegatorReward(
  apiRes: AxiosInstance,
  delegator: string,
  validator: string
) {
  try {
    const response: AxiosResponse = await apiRes.get(
      `/cosmos/distribution/v1beta1/delegators/${delegator}/rewards/${validator}`
    );
    const resData = response?.data;
    return resData?.rewards[0]?.amount || 0;
  } catch (error) {
    console.log('error getDelegatorReward', error);
    return 0;
  }

  // console.log("Res Data : ", resData?.rewards[0]?.amount);
}

// Calculate Annual Percentage Yield (APY) based on parameters
function calculateAPY(
  inflation: number,
  bondedRate: number,
  communityTax: number,
): number {
  const apy = Number(
    ((inflation / bondedRate) * (1 - communityTax) * 100).toFixed(2),
  );

  return apy;
}

const calculateData = async (): Promise<
  | {
      validator: number;
      totalValidator: number;
      bondedRate: number;
      apy: number;
      inflation: number;
      totalSupply: number;
      circulating_supply: number;
    }
  | { error: boolean; message: string }
> => {
  try {
    const apiUrl = environment.evmHost;
    const apiRes = axios.create({
      baseURL: apiUrl,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        Accept: 'application/json',
      },
      timeout: 10000,
    });

    const params = await getParams(apiRes);
    if (!params) throw new Error(RES_MSG.FETCH_SUCCESS);

    const apyValue = calculateAPY(
      Number(params.inflation),
      Number(params.bondedRate / 100),
      params.communityTax,
    );

    const validatorData = {
      validator: params?.activeValidator,
      totalValidator: params?.totalValidator,
      bondedRate: params?.bondedRate,
      apy: apyValue,
      totalSupply: params?.totalSupply,
      inflation: Number(params?.inflation),
      circulating_supply: params?.circulation_supply,
    };

    await RedisService.setString(
      REDIS_KEY.INFLATION_APY,
      JSON.stringify(validatorData),
    );

    await RedisService.setString(
      REDIS_KEY.CIRCULATING_SUPPLY,
      JSON.stringify(params?.circulation_supply),
    );

    await RedisService.setString(
      REDIS_KEY.VALIDATOR_DATA,
      JSON.stringify(validatorData),
    );
    return validatorData;
  } catch (err) {
    // logger.log('error', 'Error occurred while calculating stake APY', err);
    if (err instanceof Error) {
      return { error: true, message: err.message };
    }
    return { error: true, message: RES_MSG.NOT_FOUND };
  }
};

/**
 *  Fetch delegations for a specific validator address
 * @param apiRes
 * @param address
 * @returns
 */

/**
 * retrive unbonded amount of validator
 * @param apiRes
 * @param address
 * @returns
 */
async function validatorUnbondAmount(apiRes: AxiosInstance, address: string) {
  const response: AxiosResponse = await apiRes.get(
    `/cosmos/staking/v1beta1/validators/${address}/unbonding_delegations`,
  );
  const resData = response?.data?.unbonding_responses || [];

  let unbondedAmount = 0;
  resData.forEach((element: any) => {
    element.entries.forEach((i: any) => {
      unbondedAmount = (unbondedAmount + Number(i.balance)) / 10 ** 18;
    });
  });

  return unbondedAmount;
}

/**
 * retrive unbonded amount of validator
 * @param apiRes
 * @param address
 * @returns
 */
async function validatortotalRewards(apiRes: AxiosInstance, address: string) {
  try {
    const response: AxiosResponse = await apiRes.get(
      `cosmos/distribution/v1beta1/validators/${address}/outstanding_rewards`,
    );

    return response.data;
  } catch (err) {
    // console.log('error while validatortotalRewards : ', err);
    return null;
  }
}

/**
 *  Process and update information for each validator
 * @param apiRes
 * @returns
 */

async function getValidatorsInfo_1(apiRes: AxiosInstance) {
  try {
    const response: AxiosResponse = await apiRes.get(
      '/cosmos/staking/v1beta1/validators',
      { timeout: 10000 }
    );

    const validators: IValidatorToSave[] = [];
    const delegators: IValidatorStake[] = [];

    for (let i = 0; i < response?.data?.validators?.length; i++) {
      const validator = response.data.validators[i];
      const validatorOperatorAddress = validator.operator_address;
      const address = bech32.decode(validatorOperatorAddress);
      const delegatorAddress = bech32.encode(
        environment.address_prefix,
        address.words,
      );

      // 🟢 stake details for this validator
      const stakeDetails: any = await getValidatorStakeDetails(
        apiRes,
        validator.operator_address,
      );

      const rewardsRes = await validatortotalRewards(
        apiRes,
        validator.operator_address,
      );

      const validatorReward = Number(
        rewardsRes?.rewards?.rewards[0]?.amount || 0,
      );

      const unbondAmount = await validatorUnbondAmount(
        apiRes,
        validatorOperatorAddress,
      );

      // check self-delegation
      let selfStakeValue = 0;
      const valIndex: number = stakeDetails?.delegation_responses.findIndex(
        (e: any) =>
          e.delegation.delegator_address === delegatorAddress &&
          e.delegation.validator_address === validatorOperatorAddress
      );

      if (valIndex < 0) {
        // 🟢 fetch delegations by delegator address
        const getDelegationDetail: any = await getDelegationDetails(
          apiRes,
          delegatorAddress
        );
        console.log('getDelegationDetail',getDelegationDetail);
        selfStakeValue =
          getDelegationDetail?.delegation_responses[0]?.balance?.amount ?? 0;

        if (getDelegationDetail?.delegation_responses?.length > 0) {
          stakeDetails.delegation_responses.push(
            ...getDelegationDetail.delegation_responses
          );

          // update pagination total also
          stakeDetails.pagination.total =
            Number(stakeDetails.pagination.total) +
            getDelegationDetail.delegation_responses.length;
        }
      }

      // 🟢 resolve delegator shares / balance
      let deleShares = 0;
      if (valIndex >= 0) {
        deleShares =
          stakeDetails.delegation_responses[valIndex]?.balance?.amount || 0;
      } else if (selfStakeValue) {
        deleShares = selfStakeValue;
      }

      const selfStake = noExponent(deleShares ?? 0);
      const delegatorCount = parseInt(stakeDetails.pagination.total, 10);

      // loop through delegators
      for (let j = 0; j < delegatorCount; j++) {
        const val = stakeDetails.delegation_responses[j];
       
        let delegatorRewards = 0;

if (
  val?.delegation?.delegator_address &&
  Number(val?.balance?.amount || 0) > 0
) {
  delegatorRewards = await getDelegatorReward(
    apiRes,
    val.delegation.delegator_address,
    validatorOperatorAddress
  );
}


        // const delegatorRewards: number = await getDelegatorReward(
        //   apiRes,
        //   val?.delegation?.delegator_address,
        //   validatorOperatorAddress
        // );

        if (val?.delegation?.delegator_address) {
          const data: IValidatorStake = {
            validatorOperatorAddress,
            denom: val?.balance?.denom,
            delegatorRewards,
            stake: Number(val?.delegation?.shares),
            balanceAmount: val?.balance?.amount,
            delegatorAddress: val?.delegation?.delegator_address,
          };

          await dbHelper.saveDelegator({
            totalStake: 0.0,
            address: data.delegatorAddress,
          });

          delegators.push(data);
        }
      }

      // 🟢 validator status
      let status = RES_MSG.DEACTIVATING;
      switch (validator?.status) {
        case RES_MSG.BOND_STATUS_BONDED:
          status = RES_MSG.ACTIVE;
          break;
        case RES_MSG.BOND_STATUS_UNBONDED:
          status = RES_MSG.INACTIVE;
          break;
        case RES_MSG.BOND_STATUS_UNBONDING:
          status = RES_MSG.DEACTIVATING;
          break;
        default:
          break;
      }

      // 🟢 validator final object
      const validatorData: IValidatorToSave = {
        status,
        selfStake,
        delegatorCount,
        validatorAddress:delegatorAddress,
        jailed: validator?.jailed,
        tokens: noExponent(validator?.delegator_shares ?? 0),
        unbondingAmount: unbondAmount,
        name: validator?.description?.moniker,
        website: validator?.description?.website,
        unbondingTime: validator?.unbonding_time,
        details: validator?.description?.details,
        operatorAddress: validatorOperatorAddress,
        identity: validator?.description?.identity,
        unbondingHeight: validator?.unbonding_height,
        updatedAt: validator?.commission.update_time,
        createdAt: validator?.commission.update_time,
        unbondingIds: validator?.unbonding_ids ?? [],
        minSelfDelegation: validator?.min_self_delegation,
        SecurityContact: validator?.description?.security_contact,
        totalRewards: validatorReward ? validatorReward / 10 ** 18 : 0,
        unbondingOnHoldRefCount: validator?.unbonding_on_hold_ref_count,
        commissionUpdateTime: validator?.commission?.update_time,
        commissionRate: validator?.commission?.commission_rates.rate,
        commissionMaxRate: validator?.commission?.commission_rates?.max_rate,
        commissionMaxChangeRate:
          validator?.commission?.commission_rates?.max_change_rate,
        totalStake: noExponent(validator?.tokens ?? 0),
        votingPower: Number(validator.tokens / 10 ** 18),
      };

      validators.push(validatorData);
    }

    // save validators + delegators
    await dbHelper.saveValidators(validators);
    await dbHelper.saveDelegatorStakeData(delegators);

    return validators;
  } catch (err) {
    if (err instanceof Error) {
      console.log('Error occurred while saving validator details', err);
      return { error: true, message: err.message };
    }
    return { error: true, message: RES_MSG.VALIDATOR_ERROR };
  }

  // helper functions
  async function getDelegationDetails(
    apiUrl: AxiosInstance,
    address: string
  ): Promise<ValidatorDelegations> {
    const response: AxiosResponse = await apiUrl.get(
      `/cosmos/staking/v1beta1/delegations/${address}`
    );
    return response.data;
  }

  async function getValidatorStakeDetails(
    api: AxiosInstance,
    operatorAddress: string
  ): Promise<any> {
    let allDelegations: any[] = [];
    let nextKey: string | null = null;

    do {
      const res: AxiosResponse = await api.get(
        `/cosmos/staking/v1beta1/validators/${operatorAddress}/delegations`,
        {
          params: {
            'pagination.key': nextKey,
            'pagination.limit': 100,
          },
        }
      );

      const { data } = res;
      if (data?.delegation_responses?.length) {
        allDelegations = allDelegations.concat(data.delegation_responses);
      }

      nextKey = data?.pagination?.next_key || null;
    } while (nextKey);

    return {
      delegation_responses: allDelegations,
      pagination: {
        total: allDelegations.length, // ✅ real total
      },
    };
  }
}

async function getValidatorsInfo(api: AxiosInstance) {
  try {
    const { data } = await api.get(
      '/cosmos/staking/v1beta1/validators',
      { timeout: 30000 }
    );

    const validators: IValidatorToSave[] = [];
    const delegators: IValidatorStake[] = [];

    // 🔹 Process validators in batches (VERY IMPORTANT)
    await runInBatches(data.validators ?? [], 3, async (validator: any) => {
      const valoper = validator.operator_address;

      const [stakeDetails, validatorReward, unbondAmount] =
        await Promise.all([
          getValidatorDelegations(api, valoper),
          getValidatorTotalRewards(api, valoper),
          validatorUnbondAmount(api, valoper),
        ]);

      // Self stake
      const selfDelegation = stakeDetails.delegation_responses.find(
        (d: any) =>
          d.delegation.delegator_address.startsWith(
            environment.address_prefix
          )
      );

      const selfStake = selfDelegation
        ? noExponent(selfDelegation.balance.amount)
        : '0';

      // 🔹 Delegators batching (prevents 500 + timeout)
      await runInBatches(
        stakeDetails.delegation_responses,
        10,
        async (d: any) => {
          const delegator = d.delegation.delegator_address;
          const balanceAmount = Number(d.balance.amount || 0);
          if (!delegator || balanceAmount <= 0) return;

          const rewards = await getDelegatorRewardSafe(
            api,
            delegator,
            valoper
          );

          delegators.push({
            validatorOperatorAddress: valoper,
            delegatorAddress: delegator,
            denom: d.balance.denom,
            stake: Number(d.delegation.shares),
            balanceAmount,
            delegatorRewards: rewards,
          });

          await dbHelper.saveDelegator({
            address: delegator,
            totalStake: 0,
          });
        }
      );

      // Validator status
      let status = RES_MSG.DEACTIVATING;
      if (validator.status === RES_MSG.BOND_STATUS_BONDED) {
        status = RES_MSG.ACTIVE;
      } else if (validator.status === RES_MSG.BOND_STATUS_UNBONDED) {
        status = RES_MSG.INACTIVE;
      }

      validators.push({
        status,
        jailed: validator.jailed,
        selfStake,
        delegatorCount: stakeDetails.delegation_responses.length,
        validatorAddress: valoper,
        operatorAddress: valoper,
        tokens: noExponent(validator.tokens),
        totalStake: noExponent(validator.tokens),
        votingPower: Number(validator.tokens) / 10 ** 18,
        unbondingAmount: unbondAmount,
        name: validator.description?.moniker,
        website: validator.description?.website,
        details: validator.description?.details,
        identity: validator.description?.identity,
        SecurityContact: validator.description?.security_contact,
        unbondingHeight: validator.unbonding_height,
        unbondingTime: validator.unbonding_time,
        minSelfDelegation: validator.min_self_delegation,
        commissionRate: validator.commission?.commission_rates?.rate,
        commissionMaxRate:
          validator.commission?.commission_rates?.max_rate,
        commissionMaxChangeRate:
          validator.commission?.commission_rates?.max_change_rate,
        commissionUpdateTime: validator.commission?.update_time,
        totalRewards: validatorReward,
        updatedAt: validator.commission?.update_time,
        createdAt: validator.commission?.update_time,
        unbondingIds: validator.unbonding_ids ?? [],
        unbondingOnHoldRefCount:
          validator.unbonding_on_hold_ref_count,
      });
    });

    await dbHelper.saveValidators(validators);
    await dbHelper.saveDelegatorStakeData(delegators);

    return validators;
  } catch (err) {
    console.error('Validator indexing failed', err);
    return { error: true, message: 'VALIDATOR_FETCH_FAILED' };
  }
}


// ---------------- Helper functions ----------------
async function getValidatorDelegations(
  api: AxiosInstance,
  valoper: string
) {
  const all: any[] = [];
  let nextKey: string | null = null;

  do {
    const res: AxiosResponse = await api.get(
      `/cosmos/staking/v1beta1/validators/${valoper}/delegations`,
      {
        params: {
          'pagination.key': nextKey,
          'pagination.limit': 100,
        },
      }
    );

    all.push(...(res.data.delegation_responses || []));
    nextKey = res.data.pagination?.next_key || null;
  } while (nextKey);

  return { delegation_responses: all };
}

async function getDelegatorRewardSafe(
  api: AxiosInstance,
  delegator: string,
  validator: string
): Promise<number> {
  try {
    const res = await api.get(
      `/cosmos/distribution/v1beta1/delegators/${delegator}/rewards/${validator}`
    );

    return Number(res.data?.rewards?.[0]?.amount || 0);
  } catch (err: any) {
    const msg = err?.response?.data?.message || err?.message || '';
    if (msg.includes('delegation does not exist')) return 0;
    console.error('Reward fetch failed', { delegator, validator, err });
    return 0;
  }
}

async function getValidatorTotalRewards(
  api: AxiosInstance,
  valoper: string
): Promise<number> {
  try {
    const res = await api.get(
      `/cosmos/distribution/v1beta1/validators/${valoper}/outstanding_rewards`
    );
    return Number(res.data?.rewards?.[0]?.amount || 0);
  } catch {
    return 0;
  }
}

async function runInBatches<T>(
  items: T[],
  batchSize: number,
  fn: (item: T) => Promise<void>
) {
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    await Promise.all(batch.map(fn));
  }
}


export { calculateData, getValidatorsInfo };
