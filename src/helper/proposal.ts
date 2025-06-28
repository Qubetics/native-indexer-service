import { AxiosInstance, AxiosResponse } from "axios";
import { fetchRequest } from "../Utility/common";
import { FETCH_METHODS, HEADERS, RES_MSG } from "../constant";
import { IProposalData } from "../interface";
import dbHelper from "./db.helper";

/**
 *  Process and update information for each proposal
 * @param apiRes
 * @returns
 */
async function getProposalsInfo(apiRes: AxiosInstance) {
  try {
    const response: AxiosResponse = await apiRes.get(
      "/cosmos/gov/v1/proposals?proposal_status=PROPOSAL_STATUS_UNSPECIFIED"
    );

    const proposals: IProposalData[] = [];

    for (let i = 0; i < response?.data?.proposals?.length; i++) {
      const proposal = response.data.proposals[i];

      let status = RES_MSG.FAILED;

      switch (proposal?.status) {
        case RES_MSG.PROPOSAL_STATUS_DEPOSIT_PERIOD:
          status = RES_MSG.DEPOSIT_PERIOD;
          break;
        case RES_MSG.PROPOSAL_STATUS_VOTING_PERIOD:
          status = RES_MSG.VOTING_PERIOD;
          break;
        case RES_MSG.PROPOSAL_STATUS_PASSED:
          status = RES_MSG.PASSED;
          break;
        case RES_MSG.PROPOSAL_STATUS_REJECTED:
          status = RES_MSG.REJECTED;
          break;
        case RES_MSG.PROPOSAL_STATUS_FAILED:
          status = RES_MSG.FAILED;
          break;
        default:
          break;
      }

      const getAmount = proposal.total_deposit.find(
        (item: any) => item?.denom === `${environment.projectName}`
      );

      const messageType = proposal?.messages[0]?.["@type"];

      const proposalId = proposal?.id;

      const url = `${environment.evmHost}/cosmos/gov/v1/proposals/${proposalId}/tally`;
      const tallyData = await fetchRequest(
        url,
        FETCH_METHODS.GET,
        HEADERS.DEFAULT,
        null
      );

      const proposalData: IProposalData = {
        proposalId,
        status,
        totalDeposit: getAmount.amount,
        metaData: proposal?.metadata,
        title: proposal?.title,
        summary: proposal?.summary,
        proposer: proposal?.proposer,
        proposerType: messageType || "",
        tally: JSON.stringify(tallyData?.data?.tally), // Convert tally object to string
        votingStartTime: proposal?.voting_start_time,
        votingEndTime: proposal?.voting_end_time,
      };

      await dbHelper.saveProposals(proposalData);

      proposals.push(proposalData);
    }
    return proposals;
  } catch (err) {
    if (err instanceof Error) {
      return { error: true, message: err.message };
    }
    return {
      error: true,
      message: RES_MSG.PROPOSAL_ERROR,
    };
  }
}

export { getProposalsInfo };
