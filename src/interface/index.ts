import { TRANSACTION_STATUS, TRANSACTION_TYPE } from "../constant";

export interface IEnvironment {
  port: number;
  secretKey: string;
  databaseUrl: string;
  nodeLink: string;
  saltValue: number;
  redisHost: string;
  redisPort: string;
  rabbitMq: string;
  currency: string;
  symbol: string;
  evmHost: string;
  address_prefix: string;
  coinMarketApi: string;
  coinMarketKey: string;
  getCurrentEnvironment(): string;
  setEnvironment(env: string): void;
  isProductionEnvironment(): boolean;
  isDevEnvironment(): boolean;
  isTestEnvironment(): boolean;
  isStagingEnvironment(): boolean;
}

export interface IBlock {
  blockhash?: string;
  timestamp: string;
  blocknumber: number;
  transactionCount: number;
  miner: string;
  validatorOperatorAddress: string;
}

export interface ITransaction {
  type: string | undefined;
  status: string;
  txhash: string;
  value: string;
  txFee: string;
  gasUsed: number;
  txString: string;
  timestamp: string;
  toAddress: string;
  gasWanted: number;
  blocknumber: number;
  fromAddress: string;
  contractAddress: string;
  burnedFee?: number;
}

export interface ICOINTX {
  gas: number;
  txFee: string;
  value: string;
  txhash: string;
  gasPrice: number;
  toAddress: string;
  blockhash: string;
  timestamp: number;
  fromAddress: string;
  blocknumber: number;
  status: TRANSACTION_STATUS;
  burnedFee: number;
}

export interface IContracts {
  blockNumber: number;
  txHash: string;
  contractName: string;
  contractType: string;
  creator: string;
  // totalSupply?: number;
  // tokenSymbol?: string;
  // decimal?: number;
  address: string;
  timestamp: string;
}

export interface IESResponse {
  error: boolean;
  data?: object;
  message?: string;
}

export interface IDelegator {
  address: string;
  totalStake: number;
}

export interface IValidatorStake {
  stake: number;
  denom: string;
  // delegatorCount : number;
  // validatorAddress:string;
  delegatorRewards: number;
  delegatorAddress: string;
  balanceAmount: number;
  validatorOperatorAddress: string;
}

export interface CoinMarketData {
  price: number;
  marketCap: number;
  circulatingSupply: number;
}

export interface ITimeStamp {
  [key: string]: number;
}

export interface TotalTxResponse {
  error: boolean;
  data: {
    error: boolean;
    count: number;
  };
}

export interface Params {
  annualProvisions: number;
  blocksPerYear: number;
  inflation: string;
  bondedTokens: number;
  communityTax: number;
  activeValidator: number;
  totalValidator: number;
  circulation_supply: number;
  bondedRate: number;
  totalSupply: number;
}

export interface BlockHeader {
  height: number;
  time: Date;
}

export interface DashboardData {
  apy: number;
  validators: number;
  totalValidators: number;
  bondedRate: number;
}

export interface ValidatorDelegations {
  delegation_responses: Array<{
    delegation: {
      delegator_address: string;
      validator_address: string;
      shares: string;
    };
  }>;
}

export interface IValidator {
  name: string;
  tokens: string;
  status: string;
  website: string;
  details: string;
  jailed: boolean;
  identity: string;
  selfStake: string;
  totalStake: string;
  totalRewards: number;
  unbondingTime: string;
  unbondingIds: string[];
  delegatorCount: number;
  unbondingHeight: number;
  SecurityContact: string;
  operatorAddress: string;
  unbondingAmount: number;
  validatorAddress: string;
  minSelfDelegation: string;
  commissionRate: string;
  commissionMaxRate: string;
  commissionUpdateTime: string;
  commissionMaxChangeRate: string;
  unbondingOnHoldRefCount: string;
  votingPower: number;
}

export interface IValidatorToSave extends IValidator {
  createdAt?: string;
  updatedAt: string;
}

export interface ValidatorData {
  validator: number;
  totalValidator: number;
  bondedRate: number;
  apy: number;
}
export interface IProposalData {
  proposalId: string;
  status: string;
  totalDeposit: string;
  metaData: string;
  title: string;
  summary: string;
  proposer: string;
  proposerType: string;
  tally: string;
  votingStartTime: string;
  votingEndTime: string;
}
export interface ITokens {
  totalSupply: string;
  tokenSymbol: string;
  tokenName: string;
  decimal: string;
  creator: string;
  type: string;
  contractAddress: string;
  blockNumber: number;
}

export interface IContractLogs {
  logs: string;
  txHash: string;
  txFee: string;
  contractAddress: string;
  blockNumber: number;
  method: string;
  status: string;
  timestamp: string;
  burnedFee?: number;
}

export interface IHolders {
  address: string;
  contractAddress: string;
  tokenBalance: string;
}

export interface IVoters {
  proposalId: string;
  txHash: string;
  answer: string;
  blockNumber: number;
  voter: string;
}
