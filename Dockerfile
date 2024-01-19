# development stage
FROM node:20-alpine AS dev-stage

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

RUN npm run build && \
  npm prune --production

# production stage
FROM node:20-alpine AS production-stage

ARG PACKAGE_VERSION=untagged
ENV PACKAGE_VERSION=${PACKAGE_VERSION}
LABEL com.chrisleekr.salesforce-postgres-sync.package-version=${PACKAGE_VERSION}

WORKDIR /srv

COPY --from=build-stage /srv /srv

CMD [ "npm", "start"]
