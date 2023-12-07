FROM node:18
EXPOSE 8081
WORKDIR /app
COPY src/. .
RUN npm install
RUN chmod +x ./entrypoint.sh
ENTRYPOINT [ "./entrypoint.sh" ]
