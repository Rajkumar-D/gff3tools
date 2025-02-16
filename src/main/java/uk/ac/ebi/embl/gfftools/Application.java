package uk.ac.ebi.embl.gfftools;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.reflect.internal.Trees;
import uk.ac.ebi.embl.gfftools.model.FFAnnotation;
import uk.ac.ebi.embl.gfftools.repository.NativeGffRepository;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


@SpringBootApplication
public class Application implements CommandLineRunner {

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
        stage = args[1];
        Path outFilePath = getOutputFilePath(outputDir +"/"+primaryAcc+"/");
        setFile = outFilePath + "/set-" + primaryAcc;
        conFile =outFilePath + "/con-" + primaryAcc;
        masterFile = outFilePath+ "/master-" + primaryAcc;

        try {
            if(stage.equals("DOWNLOAD")) {
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
            } else if (stage.equals("CALCULATE")) {


                FFAnnotation setAnnotation = getAnnotationDetails(primaryAcc, setFile);
                FFAnnotation conAnnotation = Files.exists(Paths.get(conFile)) ?
                        getAnnotationDetails(primaryAcc, conFile) :
                        new FFAnnotation(primaryAcc, 0L, 0L);


                System.out.println("SET: " + setAnnotation.toString());
                System.out.println("CON: " + conAnnotation.toString());
                //nativeRepository.updateAnnotation(setAnnotation, conAnnotation);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public FFAnnotation getAnnotationDetails(String primaryAcc, String filePath) throws IOException, InterruptedException {

        System.out.println("FilePath: " + filePath);
        List<String> commandList = new ArrayList<>();
        commandList.add("/usr/bin/grep");
        commandList.add("'FT'");
        commandList.add(filePath);
        commandList.add(">");
        commandList.add(filePath+"-annotation");
        //String command = "/usr/bin/grep  'FT' "+filePath+" >> "+filePath+"-annotation";
        //executeCommand(command);

        ProcessBuilder processBuilder = new ProcessBuilder(commandList);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        Long totalAnnotationSize = Files.size(Paths.get(filePath+"-annotation"));


        commandList = new ArrayList<>();
        commandList.add("/usr/bin/grep");
        commandList.add("-e");
        commandList.add("'^FT   \\w'");
        commandList.add(filePath+"-annotation");
        commandList.add("|");
        commandList.add("/usr/bin/wc");
        commandList.add("-l");

        processBuilder = new ProcessBuilder(commandList);
        processBuilder.redirectErrorStream(true);
         process = processBuilder.start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String outLine;
        while ((outLine = reader.readLine()) != null) {
            System.out.println(outLine);
        }


        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException( " \ncommand Exit code: " + exitCode);
        }

    Long totalFeatureCount = Long.parseLong(outLine);
        //String command = "/usr/bin/grep -e \'^FT   \\w\' set-CETS01000000 | wc -l";
        //Long totalFeatureCount = Long.parseLong(executeCommand(command));

        System.out.println("Total Annotation Size: " + totalAnnotationSize);
        System.out.println("Total Feature Count: " + totalFeatureCount);
        return new FFAnnotation(primaryAcc,totalFeatureCount, totalAnnotationSize);

    }



    public String executeCommand(String command) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        String outPut = "";
        while ((line = reader.readLine()) != null) {
            outPut+=line;
        }

        // Wait for the process to exit and check for errors
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException(command + " \ncommand Exit code: " + exitCode);
        }

        return outPut;
    }

    public List<String> getGetSetffCommand(){
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
