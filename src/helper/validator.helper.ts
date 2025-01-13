import axios, { AxiosInstance, AxiosResponse } from "axios";
import { bech32 } from "bech32";
import { noExponent } from "../Utility/common";
import { RES_MSG, CONST_NAME, REDIS_KEY } from "../constant";
import {
  Params,
  // IValidator,
  ValidatorDelegations,
  IValidatorStake,
  IValidatorToSave,
} from "../interface/index";
import { RedisService } from "../service";
// import redisHelper from '../service/redis.helper';
import dbHelper from "./db.helper";

async function getParams(apiRes: AxiosInstance): Promise<Params | false> {
  try {
    const [
      validatorResponse,
      circulationSupplyResponse,
      inflationRateResponse,
      poolResponse,
      paramsResponse,
      totalSupplyResponse,
    ] = await Promise.all([
      apiRes.get("/cosmos/staking/v1beta1/validators"),
      apiRes.get("/cosmos/bank/v1beta1/supply/by_denom?denom=qubetics"),
      apiRes.get("/cosmos/mint/v1beta1/inflation"),
      apiRes.get("/cosmos/staking/v1beta1/pool"),
      apiRes.get("/cosmos/distribution/v1beta1/params"),
      apiRes.get("/cosmos/bank/v1beta1/supply"),
    ]);

    const activeValidator = validatorResponse?.data?.validators.filter(
      (item: any) => item.status === CONST_NAME.BOND_STATUS,
    );

    if (!activeValidator?.length) {
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

    const annualProvisions = Number(
      circulation_supply * parseFloat(formattedInflationRate),
    );

    let blocksPerYear = Number(CONST_NAME.BLOCK_PER_YEAR);
    blocksPerYear = Number(blocksPerYear / 2);
    const inflation = formattedInflationRate;

    const bondedTokens = Number(poolResponse.data.pool.bonded_tokens);

    // Find the entry with denom "qubetics"
    const qubeticsAmount = totalSupplyResponse.data.supply.find(
      (item: any) => item.denom === "qubetics",
    );

    const totalSupply = qubeticsAmount.amount;

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
    // logger.log('error occurred while calculating stake APY', error);
    return false;
  }
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
    }
  | { error: boolean; message: string }
> => {
  try {
    const apiUrl = environment.evmHost;
    const apiRes = axios.create({
      baseURL: apiUrl,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        Accept: "application/json",
      },
      timeout: 1000,
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
    };

    await RedisService.setString(
      REDIS_KEY.INFLATION_APY,
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
async function getValidatorStakeDetails(
  apiRes: AxiosInstance,
  address: string,
): Promise<ValidatorDelegations> {
  const response: AxiosResponse = await apiRes.get(
    `/cosmos/staking/v1beta1/validators/${address}/delegations`,
  );
  return response.data;
}

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
async function getValidatorsInfo(apiRes: AxiosInstance) {
  // : Promise<IValidator[] | { error: boolean; message: string }>
  try {
    const response: AxiosResponse = await apiRes.get(
      "/cosmos/staking/v1beta1/validators",
    );

    const validators: IValidatorToSave[] = [];
    const delegators: IValidatorStake[] = [];

    for (let i = 0; i < response?.data?.validators?.length; i++) {
      const validator = response.data.validators[i];
      const validatorOperatorAddress = validator.operator_address;
      const address = bech32.decode(validatorOperatorAddress);
      const validatorAddress = bech32.encode(
        environment.address_prefix,
        address.words,
      );

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
      const valIndex: number = stakeDetails?.delegation_responses.findIndex(
        (e: any) =>
          e.delegation.delegator_address === validatorAddress &&
          e.delegation.validator_address === validatorOperatorAddress,
      );

      const deleShares =
        stakeDetails?.delegation_responses[valIndex]?.balance.amount || 0;
      const selfStake = noExponent(deleShares ?? 0);

      const delegatorCount = parseInt(stakeDetails.pagination.total, 10);

      for (let j = 0; j < delegatorCount; j++) {
        // if (j !== valIndex) {
        const val = stakeDetails?.delegation_responses[j];
        if (val?.delegation?.delegator_address) {
          const data: IValidatorStake = {
            validatorOperatorAddress,
            denom: val?.balance?.denom,
            stake: Number(val?.delegation?.shares),
            balanceAmount: val?.balance?.amount,
            delegatorAddress: val?.delegation.delegator_address,
          };

          await dbHelper.saveDelegator({
            totalStake: 0.0,
            address: data.delegatorAddress,
          });

          delegators.push(data);
        }
        // }
      }

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

      const validatorData: IValidatorToSave = {
        status,
        selfStake,
        delegatorCount,
        validatorAddress,
        jailed: validator?.jailed,
        // tokens: (validator?.delegator_shares
        //  ? Number(validator?.delegator_shares) / 10 ** 18
        //  : 0
        // ).toString(),
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
        // totalStake: validator?.delegator_shares
        //  ? Number(validator?.delegator_shares) / 10 ** 18
        //  : 0,
        totalStake: noExponent(validator?.tokens ?? 0),
        votingPower: Number(validator.tokens / 10 ** 18),
        // totalStake: validator?.tokens
        //  ? Number(validator?.tokens) / 10 ** 18
        //  : 0,
      };

      validators.push(validatorData);
    }

    await dbHelper.saveValidators(validators);
    // console.log('delegators delegators', validators);

    await dbHelper.saveDelegatorStakeData(delegators);
    return validators;
  } catch (err) {
    console.log("Error occurred while getting validator details", err);
    if (err instanceof Error) {
      return { error: true, message: err.message };
    }
    return {
      error: true,
      message: RES_MSG.VALIDATOR_ERROR,
    };
  }
}
export { calculateData, getValidatorsInfo };
