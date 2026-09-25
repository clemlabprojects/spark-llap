// Hive Warehouse Connector — build against a given ODP release, test, publish to Nexus.
//
// Job: "Pipeline script from SCM" -> https://github.com/clemlabprojects/spark-llap.git
//      credential 'lucasbak', branch main (1.3.2 line) or branch-1.3.1, script path Jenkinsfile.
//
// Runs on 'ubuntu22' (JDK 21 required on main: Hive 4.2 artifacts are Java 21 bytecode;
// builder11 only has JDK 8). The repo bundles ./build/sbt, so no system sbt is needed.
//
// HWC COMPILES against ODP's jars, so "build for ODP 1.3.2.0-33" means resolving the
// distro coordinates ODP actually publishes: <apache-version>.<odp-version>-<build>,
// e.g. hive 4.2.0 -> 4.2.0.1.3.2.0-33. The apache versions come from that ODP build's own
// bigtop.bom, located through the git SHA in s3://clemlabs/release-info/odp/<v>-<n>.json.

@NonCPS
String bomVersion(String bom, String component) {
    // bigtop.bom blocks look like:   'hive' { ... version { version = '4.2.0' ...
    def m = (bom =~ /(?s)\n    '${java.util.regex.Pattern.quote(component)}' \{.*?version\s*=\s*'([^']+)'/)
    return m.find() ? m.group(1) : ''
}

pipeline {
    agent { label 'ubuntu22' }

    parameters {
        string(name: 'ODP_VERSION', defaultValue: '1.3.2.0', description: 'ODP line to compile against.')
        string(name: 'ODP_BUILD', defaultValue: 'latest',
               description: '"latest" = newest release-info manifest for ODP_VERSION, or pin a build number (e.g. 33).')
        booleanParam(name: 'USE_ODP_COORDS', defaultValue: true,
               description: 'Compile against the ODP-published jars. Off = the public Apache versions in build.sbt.')
        booleanParam(name: 'RUN_TESTS', defaultValue: true, description: 'Unit + integration tests (mini cluster).')
        booleanParam(name: 'PUBLISH', defaultValue: false, description: 'Publish the jar + assembly to Nexus spark-packages.')
    }

    options { timestamps(); buildDiscarder(logRotator(numToKeepStr: '30')); timeout(time: 2, unit: 'HOURS') }

    environment {
        JAVA_HOME   = '/usr/lib/jvm/java-1.21.0-openjdk-amd64'
        ODP_MAVEN   = 'https://nexus.clemlab.com/repository/maven-releases/'
        NEXUS_SPARK = 'https://nexus.clemlab.com/repository/spark-packages/'
    }

    stages {

        stage('Resolve ODP coordinates') {
            when { expression { params.USE_ODP_COORDS } }
            steps {
                script {
                    String build = params.ODP_BUILD.trim()
                    if (build == 'latest') {
                        build = sh(returnStdout: true, script: """
                            aws s3 ls s3://clemlabs/release-info/odp/ \
                              | grep -oE '${params.ODP_VERSION}-[0-9]+\\.json' \
                              | sed -E 's/.*-([0-9]+)\\.json/\\1/' | sort -n | tail -1
                        """).trim()
                        if (!build) { error "No release-info manifest for ODP ${params.ODP_VERSION}" }
                    }
                    String sha = sh(returnStdout: true, script: """
                        aws s3 cp s3://clemlabs/release-info/odp/${params.ODP_VERSION}-${build}.json - \
                          | python3 -c 'import json,sys; print(json.load(sys.stdin)["git_sha"])'
                    """).trim()
                    // One file at one commit: the contents API avoids cloning the whole stack repo.
                    String bom
                    withCredentials([string(credentialsId: 'builder', variable: 'GH_TOKEN')]) {
                        bom = sh(returnStdout: true, script: """
                            curl -fsSL -H "Authorization: Bearer \$GH_TOKEN" -H 'Accept: application/vnd.github.raw' \
                                 'https://api.github.com/repos/luc-data/odp-stack/contents/bigtop.bom?ref=${sha}'
                        """)
                    }
                    Map coords = [:]
                    for (String c : ['hive', 'spark3', 'hadoop', 'tez']) {
                        String base = bomVersion(bom, c)
                        if (!base) { error "bigtop.bom@${sha.take(10)} has no version for '${c}'" }
                        coords[c] = "${base}.${params.ODP_VERSION}-${build}"
                    }
                    env.ODP_BUILD_RESOLVED = build
                    env.HIVE_COORD = coords.hive; env.SPARK_COORD = coords.spark3
                    env.HADOOP_COORD = coords.hadoop; env.TEZ_COORD = coords.tez
                    echo "ODP ${params.ODP_VERSION}-${build} (odp-stack ${sha.take(10)}): ${coords}"

                    // Fail here with a clear message rather than deep inside sbt's resolver.
                    sh """
                        set -e
                        for gav in org/apache/hive/hive-exec/${env.HIVE_COORD} \
                                   org/apache/spark/spark-core_2.12/${env.SPARK_COORD} \
                                   org/apache/hadoop/hadoop-common/${env.HADOOP_COORD}; do
                            pom="${env.ODP_MAVEN}\$gav/\$(basename \$(dirname \$gav))-\$(basename \$gav).pom"
                            curl -sfI --max-time 20 "\$pom" >/dev/null || { echo "NOT IN NEXUS: \$pom"; exit 1; }
                            echo "found \$gav"
                        done
                    """
                }
            }
        }

        stage('Build + test') {
            steps {
                script {
                    List<String> props = []
                    if (params.USE_ODP_COORDS) {
                        // -Dresolver.url, NOT -Drepourl: repourl is also the publish target.
                        props += ["-Dresolver.url=${env.ODP_MAVEN}",
                                  "-Dhive.version=${env.HIVE_COORD}", "-Dspark.version=${env.SPARK_COORD}",
                                  "-Dhadoop.version=${env.HADOOP_COORD}", "-Dtez.version=${env.TEZ_COORD}"]
                    }
                    env.SBT_PROPS = props.join(' ')
                    // The pyspark zip is produced by a resource generator during compile, so
                    // `assembly` alone emits both the fat jar and target/pyspark_hwc-<v>.zip.
                    String goals = params.RUN_TESTS ? 'test "It / test" assembly' : 'assembly'
                    // build/sbt hardcodes `java_cmd=java` and ignores JAVA_HOME unless given -java-home;
                    // on ubuntu22 the `java` on PATH is 1.8.
                    sh """
                        set -e
                        test -x "\$JAVA_HOME/bin/java" || { echo "JDK 21 not found at \$JAVA_HOME"; exit 1; }
                        export PATH="\$JAVA_HOME/bin:\$PATH"
                        java -version
                        ./build/sbt -java-home "\$JAVA_HOME" ${env.SBT_PROPS} ${goals}
                    """
                    env.HWC_VER = sh(returnStdout: true, script:
                        "ls target/scala-2.12/hive-warehouse-connector-assembly-*.jar | sed -E 's/.*assembly-(.*)\\.jar/\\1/'").trim()
                    currentBuild.displayName = "HWC ${env.HWC_VER}" + (params.USE_ODP_COORDS ? " / ODP ${params.ODP_VERSION}-${env.ODP_BUILD_RESOLVED}" : ' / upstream')
                }
            }
        }

        stage('Publish') {
            when { expression { params.PUBLISH } }
            steps {
                withCredentials([string(credentialsId: 'jenkins_user_for_nexus', variable: 'NEXUS_PASS')]) {
                    sh '''
                        set -e
                        export PATH="$JAVA_HOME/bin:$PATH"
                        ./build/sbt -java-home "$JAVA_HOME" $SBT_PROPS -Duser=jenkins -Dpassword="$NEXUS_PASS" -Dpublish.url="$NEXUS_SPARK" publish
                    '''
                }
            }
        }
    }

    post {
        always {
            archiveArtifacts artifacts: 'target/scala-2.12/*assembly*.jar, target/pyspark_hwc-*.zip', allowEmptyArchive: true, fingerprint: true
            junit testResults: 'target/test-reports/*.xml', allowEmptyResults: true
        }
        success { slackSend channel: 'build', message: "HWC ${env.HWC_VER} OK ${env.HIVE_COORD ? '(Hive ' + env.HIVE_COORD + ')' : '(upstream)'} published=${params.PUBLISH} (<${env.BUILD_URL}|Open>)" }
        failure { slackSend channel: 'build', message: "HWC build FAILED (<${env.BUILD_URL}|Open>)" }
    }
}
