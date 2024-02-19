FROM openjdk:17
WORKDIR /wd
ARG env=prod
COPY *.jar app.jar
ENV ENV=$env
ENV JVM_OPTS="\
    -Xmx1g \
    -XX:MaxRAMPercentage=75 \
    -XX:InitialRAMPercentage=75 \
    -XX:NativeMemoryTracking=summary \
    -XX:+UseG1GC \
    -XX:G1HeapRegionSize=16m \
    -XX:+UseCompressedClassPointers \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:HeapDumpPath=/wd"
ENTRYPOINT java -Dspring.profiles.active=$ENV $JVM_OPTS -jar app.jar
