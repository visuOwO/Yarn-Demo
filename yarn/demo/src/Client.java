package demo.src;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class Client {
    public static void main(String[] args) {
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        YarnClientApplication app = yarnClient.createApplication();

        ContainerLaunchContext container =
                Records.newRecord(ContainerLaunchContext.class);

        String command =
                String.format(
                        "$JAVA_HOME/bin/java -Xmx256M %s %s 1>%s/stdout 2>%s/stderr",
                        ApplicationMaster.class.getName(),
                        "1><LOG_DIR>/stdout 2><LOG_DIR>/stderr",
                        ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
                )
        container.setCommands(Collections.singletonList(command));

        // Setup jar for ApplicationMaster
        String jar = ClassUtil.findContainingJar(Client.class);
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(jar);
        Path dst = new Path(fs.getHomeDirectory(), "yarn/" + jar);
        fs.copyFromLocalFile(false, true, src, dst);


    }
}