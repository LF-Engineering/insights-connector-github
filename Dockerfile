FROM alpine:3.14
WORKDIR /app
ENV GITHUB_ORG='<GITHUB-ORG>'
ENV GITHUB_REPO='<GITHUB-REPO>'
ENV ES_URL='<GITHUB-ES-URL>'
ENV STAGE='<STAGE>'
# RUN apk update && apk add git
RUN apk update && apk add --no-cache bash
RUN ls -ltra
COPY github ./
CMD ./git --github-org=${GITHUB_ORG} --github-repo=${GITHUB_REPO} --git-es-url=${ES_URL}
