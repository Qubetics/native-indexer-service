import * as http from "http";
import { AddressInfo } from "net";
import Environment from "./environments/environment";
import { setGlobalEnvironment } from "./global";
const env: Environment = new Environment();
setGlobalEnvironment(env);

import App from "./App";
import logger from "./lib/logger";
import { COLORS } from "./constant";
import cronService from "./cron/index";
import { log } from "./Utility/common";
import { RedisService } from "./service";
import { connectToDatabase } from "./lib/db";
import processHelper from "./helper/process.helper";
import SocketHelper from "./service/socket.heper";
import cachingHelper from "./helper/caching.helper";

declare global {
  namespace NodeJS {
    interface Global {
      socket?: Socket;
    }
  }
}

const app: App = new App();
let server: http.Server;

function serverError(error: NodeJS.ErrnoException): void {
  throw error;
}

function serverListening(): void {
  const addressInfo: AddressInfo = <AddressInfo>server.address();
  log(
    `Listening on `,
    COLORS.yellow,
    `${addressInfo.address}${env.port}`,
    COLORS.green,
  );
  connectToDatabase();
}

app
  .init()
  .then(async () => {
    await RedisService.startRedis();
    cachingHelper.initializeCount();
    cronService.checkValidators();
    cronService.saveTokenPrice();
    processHelper.downTimeBlocks();
    processHelper.subscribeLatestBlocks();
    processHelper.subscribeLatestTxs();
    // cronService.checkNullTimeStamp();

    app.express.set("port", env.port);
    server = app.httpServer;
    server.on("error", serverError);
    server.on("listening", serverListening);
    connectSocket(server);
    server.listen(env.port);
  })
  .catch((err: Error) => {
    logger.info("app.init error.");
    logger.error("err.name : ", err.name);
    logger.error("err.message : ", err.message);
    logger.error("err.stack : ", err.stack);
  });

function connectSocket(serverInstance: http.Server): void {
  const io: NodeJS.Socket = require("socket.io")(serverInstance, {
    cors: {
      origin: ["https://socket.io", "http://localhost:3000", "*"],

      credentials: true,
    },

    allowEIO3: true,
  });
  const globalData = global as NodeJS.Global;
  globalData.socket = io;
  new SocketHelper(io);
}

process.on("unhandledRejection", (reason: Error) => {
  logger.error("Unhandled Promise Rejection: reason:", reason.message);
  logger.error("reason.stack : ", reason.stack);
});
