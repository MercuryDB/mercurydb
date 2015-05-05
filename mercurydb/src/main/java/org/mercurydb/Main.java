package org.mercurydb;

import javassist.CannotCompileException;
import javassist.NotFoundException;
import org.apache.commons.cli.*;

import java.net.MalformedURLException;

public class Main {
    public static void main(String[] args)
            throws NotFoundException, CannotCompileException, MalformedURLException {

        Options options = new Options();
        Option opt = new Option("src", "source-package", true, "The source package");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("db", "db-package", true, "The output database package");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("root", "java-root", true, "The root java directory");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("dbdir", "db-directory", true, "The output directory for generated tables. (Default is relative to root)");
        options.addOption(opt);
        opt.setRequired(false);
        opt = new Option("ih", "insert-hooks", true, "Insert db hooks into package. Can specify output class directory.");
        options.addOption(opt);
        opt = new Option("sx", "suffix", true, "Specify output file suffix. Default is *Table");
        opt.setRequired(false);

        CommandLineParser parser = new GnuParser();

        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            printHelp(options);
        }

        String srcPkg = cmd.getOptionValue("src");
        String dbPkg = cmd.getOptionValue("db");
        String srcDir = cmd.getOptionValue("root");
        String suffix = cmd.getOptionValue("sfx");
        String dbDir = cmd.getOptionValue("dbdir", srcDir);

        if (srcPkg != null && dbPkg != null && srcDir != null) {
            MercuryBootstrap bs = new MercuryBootstrap(srcPkg, dbPkg, srcDir);
            if (suffix != null) {
                bs.setTableSuffix(suffix);
            }
            if (cmd.hasOption("ih")) {
                String hooksBaseDir = cmd.getOptionValue("ih", "build/classes/main");
                bs.insertBytecodeHooks(hooksBaseDir);
            } else {
                bs.generateTables(dbDir);
            }
        }
    }

    public static void printHelp(Options options) {
        new HelpFormatter().printHelp("org.mercurydb.Main", options);
        System.exit(1);
    }
}
