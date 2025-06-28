import http from "http";
import cors from "cors";
import express from "express";
import helmet from "helmet";
import registerRoutes from "./routes";

export default class App {
  public express: express.Application;

  public httpServer: http.Server;

  public async init(): Promise<void> {
    this.express = express();
    this.httpServer = http.createServer(this.express);

    // add all global middleware like cors
    this.middleware();

    // register the all routes
    this.routes();
  }

  /**
   * here register your all routes
   */
  private routes(): void {
    this.express.get("/", this.basePathRoute);
    this.express.use(`/${environment.projectName}`, registerRoutes());
  }

  /**
   * here you can apply your middlewares
   */
  private middleware(): void {
    // support application/json type post data
    // support application/x-www-form-urlencoded post data
    // Helmet can help protect your app from some well-known web vulnerabilities by setting HTTP headers appropriately.
    this.express.use(
      helmet({
        contentSecurityPolicy: false,
        crossOriginEmbedderPolicy: false,
      }),
    );
    this.express.use(express.json({ limit: "100mb" }));
    this.express.use(express.urlencoded({ limit: "100mb", extended: true }));
    // add multiple cors options as per your use
    const corsOptions = {
      origin: [],
    };
    this.express.use(cors(corsOptions));
  }

  private basePathRoute(
    _request: express.Request,
    response: express.Response,
  ): void {
    response.json({ message: "base path" });
  }
}
