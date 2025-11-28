import moment from "moment";

export const REGEX = {
  numbersOnly: /\d+(\.\d+)?/,
};

export const RES_MSG = {
  NOT_FOUND: "data not found",
  FETCH_SUCCESS: "data fetched successfully",
  ACTIVE: "active",
  INACTIVE: "inactive",
  DEACTIVATING: "deactivating",
  BOND_STATUS_BONDED: "BOND_STATUS_BONDED",
  BOND_STATUS_UNBONDED: "BOND_STATUS_UNBONDED",
  BOND_STATUS_UNBONDING: "BOND_STATUS_UNBONDING",
  VALIDATOR_ADDED: "validator added successfully!",
  VALIDATOR_UPDATED: "validator added successfully!",
  VALIDATOR_ADDED_ERROR: "error while adding validator!",
  VALIDATOR_ERROR: "Error occurred while getting validator details",
  PROPOSAL_ERROR: "Error occurred while getting proposal details",
  APR_VALUE_ERROR: "error occur while fetching APR value and validators count",
  SERVER_ERROR: "server error",
  USER_ADDED: "user save succeesfully",
  CONTRACT_ADDED: "contract saved successfully",
  TOKEN_ADDED: "token added successfully!",
  TOKEN_ADDED_ERROR: "error while saving token!",
  ERROR: "Oops! Something went wrong. Please try again.",
  PROPOSAL_STATUS_DEPOSIT_PERIOD: "PROPOSAL_STATUS_DEPOSIT_PERIOD",
  PROPOSAL_STATUS_VOTING_PERIOD: "PROPOSAL_STATUS_VOTING_PERIOD",
  PROPOSAL_STATUS_PASSED: "PROPOSAL_STATUS_PASSED",
  PROPOSAL_STATUS_REJECTED: "PROPOSAL_STATUS_REJECTED",
  PROPOSAL_STATUS_FAILED: "PROPOSAL_STATUS_FAILED",
  VOTING_PERIOD: "voting",
  DEPOSIT_PERIOD: "deposit",
  PASSED: "passed",
  REJECTED: "rejected",
  FAILED: "failed",
  VOTER_SAVED: "voter saved successfully!",
  VOTER_SAVED_ERROR: "error while saving voter!",
  VOTE_OPTION_YES: "VOTE_OPTION_YES",
  YES: "yes",
  VOTE_OPTION_NO: "VOTE_OPTION_NO",
  NO: "no",
  VOTE_OPTION_NO_WITH_VETO: "VOTE_OPTION_NO_WITH_VETO",
  VETO: "veto",
  VOTE_OPTION_ABSTAIN: "VOTE_OPTION_ABSTAIN",
  ABSTAIN: "abstain",
};

export const ERROR_MSG = {
  GET_BLOCK_INFO_ERROR: "error occured while fetching block number",
  GET_TX_ERROR: "error occured while fetching transaction detail",
  GET_CONTRACT_ERROR: "error occured while fetching contract detail",
  DECODE_PARAM_ERROR: "error occured while decoding param",
  GET_APR_ERROR: "error occured while calculating apr value",
};

export const CONST_NAME = {
  GET_MIN: moment(new Date()).format("mm"),
  GET_HOUR: moment(new Date()).format("hh"),
  GET_SEC: moment().format("ss"),
  POWER: 10 ** 18,
  NA: "N/A",
  BOND_STATUS: "BOND_STATUS_BONDED",
  YEAR_MILI_SEC: 31536000000,
  BLOCK_PER_YEAR: 365 * 24 * 60 * 60,
  ETH_EXP: 10 ** 18,
  ONE: 1,
};

export const QUEUE_NAME = {
  LATEST_TXS: "NATIVE_LATEST_TXS",
  MISSED_TXS: "NATIVE_MISSED_TXS",
  ERROR_BLOCK: "NATIVE_ERROR_BLOCK",
  MISSED_BLOCK: "NATIVE_MISSED_BLOCK",
  LATEST_BLOCKS: "NATIVE_LATEST_BLOCKS",
  DOWNTIME_BLOCK: "NATIVE_DOWNTIME_BLOCK",
  DOWNTIME_TXS: "NATIVE_DOWNTIME_TXS",
  LATEST_PROPOSALS: "LATEST_PROPOSALS",
  NULL_TIMESTAMP: "NULL_TIMESTAMP",
};

export const REDIS_KEY = {
  MARKET_CAP: "MARKET_CAP",
  MARKET_CAP_PRICE: "MARKET_CAP_PRICE",
  CIRCULATING_SUPPLY: "CIRCULATING_SUPPLY",
  TOKEN_PRICE: "TOKEN_PRICE",
  LATEST_BLOCK: "LATEST_BLOCK",
  INFLATION_APY: "INFALTION_AND_APY",
  VALIDATOR_DETAILS: "VALIDATORS_DETAILS",
  DELEGATORS_DETAILS: "DELEGATORS_DETAILS",
  VALIDATOR_ID_COUNTER: "VALIDATOR_ID_COUNTER",
  DELEGATOR_ID_COUNTER: "DELEGATOR_ID_COUNTER",
  ACCOUNT_GRAPH: "ACCOUNT_GRAPH_PAYLOAD",
  TPS_GRAPH: "TPS_GRAPH_PAYLOAD",
  CONTRACT_DEPLOY_GRAPH: "CONTRACT_DEPLOY_GRAPH_PAYLOAD",
  NATIVE_BLOCKS_COUNT: "NATIVE_BLOCKS_COUNT",
  NATIVE_TRANSACTIONS_COUNT: "NATIVE_TRANSACTIONS_COUNT",
  VALIDATOR_DATA: "VALIDATOR_DATA",
  COIN_PRICE: "COIN_PRICE",
  TOKEN_GRAPH: "TOKEN_GRAPH_PAYLOAD",
  REWARDS: "REWARDS",
};

export const SOCKET_EVENT = {
  NATIVE_LATEST_BLOCK: "NATIVE_LATEST_BLOCK",
  NATIVE_LATEST_TRANSACTIONS: "NATIVE_LATEST_TRANSACTIONS",
  ALL_TRANSACTIONS: "NATIVE_ALL_TRANSACTIONS_GRAPH",
  ACCOUNT_TX: "NATIVE_ACCOUNT_TX_GRAPH",
  CONTRACT_DEPLOYED_TX: "NATIVE_CONTRACT_DEPLOYED_GRAPH",
  TX_SUMMARY: "NATIVE_TX_SUMMARY",
  ANALYTICS: "NATIVE_ANALYTICS",
};

export enum TRANSACTION_STATUS {
  PENDING = "PENDING",
  SUCCESS = "SUCCESS",
  FAILED = "FAILED",
}

export enum TRANSACTION_TYPE {
  UNJAIL = "UNJAIL",
  BOND_MORE = "BOND_MORE",
  UNBOND_FUNDS = "UNBOND_FUNDS",
  COIN_TRANSFER = "COIN_TRANSFER",
  EDIT_VALIDATOR = "EDIT_VALIDATOR",
  CREATE_VALIDATOR = "CREATE_VALIDATOR",
  CREATE_DELEGATOR = "CREATE_DELEGATOR",
  BEGIN_REDELEGATE = "BEGIN_REDELEGATE",
  CANCEL_UNBONDING_DELEGATION = "CANCEL_UNBONDING_DELEGATION",
  CONTRACT_UPLOAD = "CONTRACT_UPLOAD",
  CONTRACT_CREATION = "CONTRACT_CREATION",
  CONTRACT_EXECUTE = "CONTRACT_EXECUTE",
  DELEGATOR_REWARD = "DELEGATOR_REWARD",
  VALIDATOR_COMMISSION = "VALIDATOR_COMMISSION",
  IBC_TRANSFER_INITIATE = "IBC_TRANSFER_INITIATE",
  RESTAKING_EXECUTION = "RESTAKING_EXECUTION",
  IBC_TRANSFER_RECEIVE = "IBC_TRANSFER_RECEIVE",
  IBC_TRANSFER_CONFIRM = "IBC_TRANSFER_CONFIRM",
  SUBMIT_PROPOSAL = "SUBMIT_PROPOSAL",
  VOTING = "VOTING",
  COIN_RECEIVE = "COIN_RECEIVE",
  START_SUBSCRIPTION = "START_SUBSCRIPTION",
  END_SUBSCRIPTION = "END_SUBSCRIPTION",
  START_REQUEST = "START_REQUEST",
  END_REQUEST = "END_REQUEST",
  NODE_EARNING = "NODE_EARNING",
}

export const HEADERS = {
  DEFAULT: {
    "Content-Type": "application/json",
    Accept: "application/json",
  },
};

export const FETCH_METHODS = {
  POST: "POST",
  GET: "GET",
  PATCH: "PATCH",
  PUT: "PUT",
};

export const COLORS = {
  reset: "\x1b[0m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  magenta: "\x1b[35m",
  cyan: "\x1b[36m",
  white: "\x1b[37m",
};

export const CURRENT_DEFAULT_COIN_PRICE = 0.337097;
export const CURRENT_DEFAULT_CIRCULATING_SUPPLY = 30542186;

export const TYPE_MAP: Record<string, string> = {
  "/qubetics.plan.v3.MsgCreatePlanRequest": "CREATE_PLAN",
  "/qubetics.plan.v3.MsgUpdatePlanStatusRequest": "UPDATE_PLAN_STATUS",
  "/qubetics.lease.v1.MsgStartLeaseRequest": "START_LEASE",
  "/qubetics.node.v3.MsgUpdateNodeStatusRequest": "UPDATE_NODE_STATUS",
  "/qubetics.plan.v3.MsgLinkNodeRequest": "LINK_NODE",
  "/qubetics.plan.v3.MsgUnlinkNodeRequest": "UNLINK_NODE",
  "/qubetics.lease.v1.MsgEndLeaseRequest": "End_Lease",
  "/qubetics.session.v3.MsgUpdateSessionRequest": "Session_Updated",
};
