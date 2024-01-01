# development stage
FROM node:20-alpine AS dev-stage

RUN apk add --no-cache make gcc g++ py-pip

WORKDIR /srv

COPY package*.json ./

RUN npm install

COPY . .

ARG PACKAGE_VERSION=untagged
ENV PACKAGE_VERSION=${PACKAGE_VERSION}
LABEL com.chrisleekr.salesforce-postgres-sync.package-version=${PACKAGE_VERSION}

CMD [ "node", "app/index.js" ]

# build stage
FROM dev-stage AS build-stage

RUN npm run build

RUN rm -rf node_modules

RUN npm install --production

# production stage
FROM node:20-alpine AS production-stage

ARG PACKAGE_VERSION=untagged
ENV PACKAGE_VERSION=${PACKAGE_VERSION}
LABEL com.chrisleekr.salesforce-postgres-sync.package-version=${PACKAGE_VERSION}

WORKDIR /srv

COPY --from=build-stage /srv /srv


CMD [ "npm", "start"]
