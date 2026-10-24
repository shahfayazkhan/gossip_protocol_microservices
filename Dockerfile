FROM node:22-alpine
WORKDIR /app
COPY package.json ./
COPY src ./src
COPY bin ./bin
COPY public ./public
ENV NODE_ENV=production BIND_HOST=0.0.0.0
CMD ["node", "bin/node.js"]
