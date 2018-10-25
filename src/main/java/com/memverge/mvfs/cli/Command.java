/*
 * Copyright (c) 2018 MemVerge Inc.
 */

package com.memverge.mvfs.cli;

import static java.util.Arrays.asList;

import com.memverge.mvfs.connection.MVFSConnectionMgr;
import com.memverge.mvfs.dmo.DMOFile;
import com.memverge.mvfs.dmo.DMOOutputStream;
import com.memverge.mvfs.utils.Printer;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Slf4j
public class Command {

  private static final Printer printer = Printer.get();

  private final String[] args;
  private final String subCommand;

  private Command(String[] args) {
    if (args.length > 0) {
      this.subCommand = args[0];
      this.args = Arrays.copyOfRange(args, 1, args.length);
    } else {
      this.subCommand = "";
      this.args = new String[0];
    }
  }

  @Getter(lazy = true)
  private final Options options = initOptions();

  private void printHelp() {
    val formatter = new HelpFormatter();
    formatter.printHelp("jmvfs <sub-command> <options>", getOptions());
  }

  private Options initOptions() {
    val options = new Options();
    options.addOption("s", "socket", true,
        "Domain socket path to connect");
    options.addOption("f", "file", true,
        "Input file for use case to test");
    options.addOption("i", "id", true,
        "ID of the DMO.");
    options.addOption("t", "target", true,
        "Target file to write to.");
    options.addOption("cs", "ChunkSize", true,
        "Chunk size of the object.");
    return options;
  }

  private Collection<String> getSubCommands() {
    return asList(
        "help",
        "attribute",
        "create",
        "read",
        "write",
        "append",
        "delete",
        "rename",
        "list",
        "listAll",
        "reset"
    );
  }

  private CommandLine parse() {
    val parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(getOptions(), args);
    } catch (UnrecognizedOptionException e) {
      printer.println(e.getMessage());
      printHelp();
    } catch (MissingOptionException e) {
      printer.println(e.getMessage());
      log.warn("Lack of required option.", e);
      printHelp();
    } catch (ParseException e) {
      log.error(e.getMessage(), e);
    }
    return cmd;
  }

  private Command execute() {
    val cmd = parse();
    if (cmd != null) {
      val filePath = cmd.getOptionValue("f", "");
      val socketPath = cmd.getOptionValue("s", "");
      val dmoId = cmd.getOptionValue("i", "/");
      val targetPath = cmd.getOptionValue("t", "");
      val chunkSize = Integer.parseInt(cmd.getOptionValue("cs", "0"));
      val inputs = StringUtils.join(cmd.getArgList(), " ");

      log.info("execute command with -f: '{}', -s: '{}', -i: '{}', -t: '{}', "
              + "-ts: '{}', inputs: '{}'",
          filePath, socketPath, dmoId, targetPath, chunkSize, inputs);
      try {
        DMOFile dmoFile = new DMOFile(socketPath, dmoId, chunkSize);
        switch (subCommand) {
          case "help":
          case "h":
            printHelp();
            break;
          case "attribute":
          case "attr":
            printAttr(dmoFile);
            break;
          case "create":
          case "c":
            dmoFile.create();
            break;
          case "read":
          case "r":
            readFile(dmoFile, targetPath);
            break;
          case "write":
          case "w":
            write(filePath, inputs, dmoFile);
            break;
          case "import":
          case "im":
            importData(filePath, dmoFile, chunkSize);
            break;
          case "append":
          case "a":
            append(filePath, inputs, dmoFile);
            break;
          case "delete":
          case "d":
            dmoFile.delete();
            break;
          case "rename":
          case "rn":
            rename(dmoFile, targetPath);
            break;
          case "list":
          case "ls":
          case "l":
            printList(dmoFile.list());
            break;
          case "listAll":
          case "la":
            printList(DMOFile.listAll(socketPath));
            break;
          case "reset":
          case "rs":
            DMOFile.reset(socketPath);
            break;
          default:
            val subCommands = getSubCommands();
            printer.println("Unknown sub-command: '%s'", subCommand);
            printer.println("Supported sub-commands are: %s",
                StringUtils.join(subCommands, ", "));
        }
      } catch (IOException ex) {
        log.error("Failed to execute the command.\n\nError Details:\n{}",
            ex.getMessage(), ex);
      }
    }
    return this;
  }

  private void rename(DMOFile dmoFile, String targetPath) throws IOException {
    if (targetPath.isEmpty()) {
      printer.println("Please specify rename target with -t.");
    } else {
      dmoFile.rename(targetPath);
    }
  }

  private void printList(String[] nameList) {
    if (ArrayUtils.isEmpty(nameList)) {
      printer.println("<empty>");
    } else {
      printer.println(StringUtils.join(nameList, "\n"));
    }
  }

  private void append(String filePath, String inputs, DMOFile dmoFile)
      throws IOException {
    if (StringUtils.isEmpty(filePath)) {
      try (val dmoOs = new DMOOutputStream(dmoFile, true)) {
        dmoOs.write(inputs.getBytes());
      }
    } else {
      dmoFile.append(new File(filePath));
    }
  }

  private void write(String filePath, String inputs, DMOFile dmoFile)
      throws IOException {
    if (StringUtils.isEmpty(filePath)) {
      try (val dmoOs = new DMOOutputStream(dmoFile)) {
        dmoOs.write(inputs.getBytes());
      }
    } else {
      dmoFile.write(new File(filePath));
    }
  }

  private String getProtocol(String filePath) {
    return filePath.split(":")[0];
  }

  private boolean isHDFSFile(String filePath) {
    return getProtocol(filePath).equalsIgnoreCase("hdfs");
  }

  /**
   * Import data from source to DMO folder
   *
   * If source is a file, we will write it to DMO folder directly If source is a directory, we will
   * recursively iterate the directory, import contents under it to DMO with the same structure.
   *
   * For HDFS and local dir, we have the same strategy.
   */
  private void importData(String filePath, DMOFile dmoFile, int chunkSize) throws IOException {
    if (!dmoFile.isFolder()) {
      throw new RuntimeException("Please specify one DMO folder to be imported");
    }
    if (!dmoFile.exists()) {
      dmoFile.create();
    }

    if (isHDFSFile(filePath)) {
      val conf = new Configuration();
      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      val path = new Path(filePath);
      val fs = FileSystem.get(URI.create(filePath), conf);
      if (fs.exists(path)) {
        if (fs.isDirectory(path)) {
          importHDFSFolder(fs, path, dmoFile, chunkSize);
        } else {
          importHDFSFile(fs, path, dmoFile, chunkSize);
        }
      } else {
        printer.println("%s doesn't exist", filePath);
      }
    } else {
      val file = new File(filePath);
      if (file.exists()) {
        if (file.isDirectory()) {
          importLocalDir(file, dmoFile, chunkSize);
        } else {
          importLocalFile(file, dmoFile, chunkSize);
        }
      } else {
        printer.println("%s doesn't exist", filePath);
      }
    }
  }

  private void importHDFSFolder(FileSystem fs, Path hdfsFolder, DMOFile dmoFile, int chunkSize)
      throws IOException {
    for (FileStatus file : fs.listStatus(hdfsFolder)) {
      printer.println("Ready to import %s", file.getPath().toString());
      val newDMOFile = getChildFile(dmoFile, file.getPath().getName(), chunkSize);
      if (fs.isDirectory(file.getPath())) {
        importHDFSFile(fs, file.getPath(), newDMOFile, chunkSize);
      } else {
        importHDFSFile(fs, file.getPath(), newDMOFile, chunkSize);
      }
    }
  }

  private void importHDFSFile(FileSystem fs, Path hdfsFile, DMOFile dmoFile, int chunkSize)
      throws IOException {
    log.debug("Read hdfs file {} into DMO", hdfsFile);
    val dstFile =
        dmoFile.isFolder() ? getChildFile(dmoFile, hdfsFile.getName(), chunkSize) : dmoFile;
    //Init input stream
    try (val dmoOs = new DMOOutputStream(dstFile)) {
      try (FSDataInputStream inputStream = fs.open(hdfsFile)) {
        IOUtils.copy(inputStream, dmoOs);
      }
    }
  }

  private void importLocalDir(File dirFile, DMOFile dmoFile, int chunkSize) throws IOException {
    for (val f : Objects.requireNonNull(dirFile.listFiles())) {
      val newDMOFile = getChildFile(dmoFile, f.getName(), chunkSize);
      if (f.isDirectory()) {
        importLocalDir(f, newDMOFile, chunkSize);
      } else {
        importLocalFile(f, newDMOFile, chunkSize);
      }
    }
  }

  private void importLocalFile(File localFile, DMOFile dmoFile, int chunkSize) throws IOException {
    if (dmoFile.isFolder()) {
      getChildFile(dmoFile, localFile.getName(), chunkSize).write(localFile);
    } else {
      dmoFile.write(localFile);
    }
  }

  private DMOFile getChildFile(DMOFile dmoFile, String childName, int chunkSize)
      throws IOException {
    val newDMOFile = new DMOFile(dmoFile.getSocketPath(), dmoFile.getDmoId() + childName,
        chunkSize);
    newDMOFile.create();
    return newDMOFile;
  }

  private void printAttr(DMOFile dmoFile) throws IOException {
    val attr = dmoFile.attribute();
    if (attr.isValid()) {
      printer.println("Attributes for '%s'", dmoFile.getDmoId());
      printer.println(attr.toString());
    }
  }

  private void readFile(DMOFile dmoFile, String targetPath) throws IOException {
    if (dmoFile.isFolder()) {
      if (StringUtils.isEmpty(targetPath)) {
        printer.println("Source is a directory.  Please specify target with -t.");
      } else {
        FileUtils.forceMkdir(new File(targetPath));
        val children = dmoFile.list();
        Arrays.stream(children).parallel()
            .forEach(child -> {
              val childTargetPath = Paths.get(targetPath, child).toString();
              try {
                DMOFile childFile = new DMOFile(dmoFile.getPath(child));
                readFile(childFile, childTargetPath);
              } catch (IOException ex) {
                log.error("Failed to copy child file {}/{} to target {}.",
                    dmoFile, child, childTargetPath, ex);
              }
            });
      }
    } else {
      val bytes = dmoFile.read();
      if (StringUtils.isEmpty(targetPath)) {
        printer.printRaw(new String(bytes));
      } else {
        try (val fos = new FileOutputStream(new File(targetPath))) {
          fos.write(bytes);
        }
      }
    }
  }

  private void cleanup() {
    MVFSConnectionMgr.cleanup();
  }

  public static void main(String[] args) {
    new Command(args).execute().cleanup();
  }
}
