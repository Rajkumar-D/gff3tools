package uk.ac.ebi.embl.gfftools;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import uk.ac.ebi.embl.gfftools.model.FFAnnotation;
import uk.ac.ebi.embl.gfftools.repository.NativeGffRepository;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;


@SpringBootApplication
public class Application implements CommandLineRunner {

    Logger logger = LoggerFactory.getLogger(Application.class);
    @Autowired
    NativeGffRepository nativeRepository;

    @Value("${sequencepersistenceclient.jar.path}")
    private String jarPath;

    @Value("${out.dir}")
    private String outputDir;

    @Value("${db.url}")
    private String databaseUrl;

    @Value("${spark.master}")
    private String sparkMaster;

    public static final Pattern featurePattern = Pattern.compile("^FT\\s{3}\\S+");


    String primaryAcc;
    String setFile;
    String conFile;
    String masterFile;
    String stage;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }


    @Override
    public void run(String... args) throws Exception {

        primaryAcc = args[0];
        Path outFilePath = getOutputFilePath(outputDir +"/"+primaryAcc+"/");
        setFile = outFilePath + "/set-" + primaryAcc;
        conFile =outFilePath + "/con-" + primaryAcc;
        masterFile = outFilePath+ "/master-" + primaryAcc;

        try {

                List<String> command = getGetSetffCommand();

                ProcessBuilder processBuilder = new ProcessBuilder(command);
                processBuilder.redirectErrorStream(true);
                Process process = processBuilder.start();

                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String outLine;
                while ((outLine = reader.readLine()) != null) {
                    System.out.println(outLine);
                }

                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    throw new RuntimeException(command + " \ncommand Exit code: " + exitCode);
                }

                assert Files.exists(Paths.get(setFile));

                FFAnnotation setAnnotation = getAnnotationDetails(primaryAcc, setFile);
                FFAnnotation conAnnotation = Files.exists(Paths.get(conFile)) ?
                        getAnnotationDetails(primaryAcc, conFile) :
                        new FFAnnotation(primaryAcc, 0L, 0L);

                nativeRepository.updateAnnotation(setAnnotation, conAnnotation);
            System.out.println("############################################");
            System.out.println("SET: " + setAnnotation.toString());
            System.out.println("CON: " + conAnnotation.toString());
            System.out.println("############################################");


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public FFAnnotation getAnnotationDetails(String primaryAcc, String filePath) throws IOException {

        System.out.println("FilePath: " + filePath);
        long totalFeatureCount = 0;
        long totalAnnotationSize = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.matches("^FT.*")) {
                    totalAnnotationSize += line.getBytes().length;
                    if (featurePattern.matcher(line).find()){
                        totalFeatureCount++;
                    }
                }
            }
            return new FFAnnotation(primaryAcc, totalFeatureCount, totalAnnotationSize);
        }catch (Exception e){
            throw new RuntimeException("Error while getting annotation: "+e);
        }
    }

    public List<String> getGetSetffCommand() {
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-cp");
        command.add(jarPath);
        command.add("uk.ac.ebi.embl.flatfile.cli.GetSetffApplication");
        command.add(databaseUrl);
        command.add(primaryAcc);
        command.add("-set");
        command.add(setFile);
        command.add("-con");
        command.add(conFile);
        command.add("-master");
        command.add(masterFile);
        command.add("-non_public");
        ;

        logger.info(command.toString());

        return command;
    }

    private Path getOutputFilePath(String filePath){
        try {
            if(Files.exists(Paths.get(filePath))){
               return Paths.get(filePath);
            }
            return Files.createDirectories(Paths.get(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
