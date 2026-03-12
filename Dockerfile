FROM node:18-alpine
# ENV NODE_ENV production

WORKDIR /usr/src/app

COPY --chown=node:node . /usr/src/app

RUN mkdir -p ./logs
RUN chown node ./logs

RUN apk add --no-cache --virtual .build-deps alpine-sdk python3
RUN apk add --no-cache openssl

# Install dependencies ignoring peer dependency conflicts
RUN npm install --legacy-peer-deps --ignore-scripts

RUN npm run generate:prisma

RUN npm rebuild

RUN apk del .build-deps

RUN npm run build:prod

USER node

EXPOSE 7500

CMD ["npm", "run", "start:production"]