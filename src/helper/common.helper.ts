import axios, { AxiosResponse, Method } from "axios";
import logger from "../lib/logger";

// need to remove
const fetchRequest = async (
  url: string,
  data: { symbol: string; convert: string },
  _method: Method,
  headers: object,
) =>
  new Promise((resolve, reject) => {
    try {
      const { symbol } = data;
      axios
        .get(url, {
          params: data,
          headers,
        })
        .then((res: AxiosResponse) => {
          const coinMarketPrice = {
            price: res.data.data[symbol]?.quote?.USD.price,
            volume24h: res.data.data[symbol]?.quote.USD.volume_24h,
            marketCap: res.data.data[symbol]?.quote.USD.market_cap,
            circulatingSupply: res.data.data[symbol]?.circulating_supply,
            volumeChange24h:
              res.data.data[symbol]?.quote?.USD.volume_change_24h,
            percentChange1h:
              res.data.data[symbol]?.quote?.USD.percent_change_1h,
            percentChange24h:
              res.data.data[symbol]?.quote?.USD.percent_change_24h,
            percentChange7d:
              res.data.data[symbol]?.quote?.USD.percent_change_7d,
            percentChange30d:
              res.data.data[symbol]?.quote?.USD.percent_change_30d,
          };
          resolve(coinMarketPrice);
        })
        .catch((err) => {
          logger.error(
            "Error in catch of coin http : ",
            err.response.data.status.error_message,
          );
          console.error("Error in catch of coin http : ", err);
        });
    } catch (err) {
      logger.error(`\x1b[31mError in http request helper: ${err}\x1b[31m`);
      reject(err);
    }
  });

export { fetchRequest };
