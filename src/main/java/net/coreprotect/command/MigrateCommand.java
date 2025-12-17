package net.coreprotect.command;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.Locale;

import org.bukkit.command.CommandSender;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.database.Database;
import net.coreprotect.language.Phrase;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.Color;

public class MigrateCommand extends Consumer {

    protected static void runCommand(final CommandSender player, boolean permission, String[] args) {
        if (ConfigHandler.converterRunning || ConfigHandler.migrationRunning) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.UPGRADE_IN_PROGRESS));
            return;
        }
        if (ConfigHandler.purgeRunning) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.PURGE_IN_PROGRESS));
            return;
        }
        if (!permission) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.NO_PERMISSION));
            return;
        }

        // Parse command arguments: /co migrate <source> <target>
        if (args.length < 3) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.MISSING_PARAMETERS, "/co migrate <sqlite|h2> <sqlite|h2>"));
            return;
        }

        String sourceType = args[1].toLowerCase(Locale.ROOT);
        String targetType = args[2].toLowerCase(Locale.ROOT);

        // Validate source and target types
        if (!isValidDatabaseType(sourceType) || !isValidDatabaseType(targetType)) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.MIGRATE_INVALID_TYPE));
            return;
        }

        if (sourceType.equals(targetType)) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.MIGRATE_SAME_TYPE));
            return;
        }

        // Check if MySQL is in use (can't migrate to/from MySQL with this command)
        if (Config.getGlobal().MYSQL) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.MIGRATE_MYSQL_ACTIVE));
            return;
        }

        // Verify source database exists
        if (!sourceExists(sourceType)) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.MIGRATE_SOURCE_NOT_FOUND, sourceType.toUpperCase()));
            return;
        }

        // Check if target database already exists
        if (targetExists(targetType)) {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.MIGRATE_TARGET_EXISTS, targetType.toUpperCase()));
            return;
        }

        // Run migration in a separate thread
        final String source = sourceType;
        final String target = targetType;

        class MigrationThread implements Runnable {
            @Override
            public void run() {
                try {
                    performMigration(player, source, target);
                }
                catch (Exception e) {
                    Chat.sendGlobalMessage(player, Phrase.build(Phrase.MIGRATE_FAILED));
                    e.printStackTrace();
                }
            }
        }

        Runnable runnable = new MigrationThread();
        Thread thread = new Thread(runnable);
        thread.start();
    }

    private static boolean isValidDatabaseType(String type) {
        return type.equals("sqlite") || type.equals("h2");
    }

    private static boolean sourceExists(String type) {
        if (type.equals("sqlite")) {
            File sqliteFile = new File(ConfigHandler.path + ConfigHandler.sqlite);
            return sqliteFile.exists();
        }
        else if (type.equals("h2")) {
            File h2File = new File(ConfigHandler.path + ConfigHandler.h2database + ".mv.db");
            return h2File.exists();
        }
        return false;
    }

    private static boolean targetExists(String type) {
        if (type.equals("sqlite")) {
            File sqliteFile = new File(ConfigHandler.path + ConfigHandler.sqlite);
            return sqliteFile.exists();
        }
        else if (type.equals("h2")) {
            File h2File = new File(ConfigHandler.path + ConfigHandler.h2database + ".mv.db");
            return h2File.exists();
        }
        return false;
    }

    private static void performMigration(CommandSender player, String sourceType, String targetType) throws Exception {
        Chat.sendGlobalMessage(player, Phrase.build(Phrase.MIGRATE_STARTED, sourceType.toUpperCase(), targetType.toUpperCase()));
        Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_NOTICE_1));
        Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_NOTICE_2));

        ConfigHandler.migrationRunning = true;

        // Pause consumer
        while (!Consumer.pausedSuccess) {
            Thread.sleep(1);
        }
        Consumer.isPaused = true;

        Connection sourceConnection = null;
        Connection targetConnection = null;

        try {
            // Get source connection
            sourceConnection = getSourceConnection(sourceType);
            if (sourceConnection == null) {
                Chat.sendGlobalMessage(player, Phrase.build(Phrase.DATABASE_BUSY));
                return;
            }

            // Get target connection
            targetConnection = getTargetConnection(targetType);
            if (targetConnection == null) {
                Chat.sendGlobalMessage(player, Phrase.build(Phrase.DATABASE_BUSY));
                return;
            }

            // Create tables in target database
            createTargetTables(targetConnection, targetType);

            // Migrate each table
            long totalRows = 0;
            int tableCount = ConfigHandler.databaseTables.size();
            int currentTable = 0;
            
            for (String table : ConfigHandler.databaseTables) {
                currentTable++;
                String tableName = table.replaceAll("_", " ");
                int percentage = (int) ((currentTable * 100.0) / tableCount);
                
                Chat.sendGlobalMessage(player, Phrase.build(Phrase.MIGRATE_PROCESSING, tableName));

                long rows = migrateTable(sourceConnection, targetConnection, ConfigHandler.prefix + table, targetType, player);
                totalRows += rows;

                if (rows > 0) {
                    String rowCount = NumberFormat.getInstance().format(rows);
                    Chat.sendGlobalMessage(player, Phrase.build(Phrase.PURGE_ROWS, rowCount));
                }
                
                // Show progress percentage
                Chat.sendGlobalMessage(player, Phrase.build(Phrase.MIGRATE_PROGRESS, String.valueOf(percentage), String.valueOf(currentTable), String.valueOf(tableCount)));
            }

            String totalRowCount = NumberFormat.getInstance().format(totalRows);
            Chat.sendGlobalMessage(player, Phrase.build(Phrase.MIGRATE_SUCCESS, totalRowCount, sourceType.toUpperCase(), targetType.toUpperCase()));
            Chat.sendGlobalMessage(player, Phrase.build(Phrase.MIGRATE_RESTART));
        }
        finally {
            if (sourceConnection != null) {
                try {
                    sourceConnection.close();
                }
                catch (Exception e) {
                    // ignore
                }
            }
            if (targetConnection != null) {
                try {
                    targetConnection.close();
                }
                catch (Exception e) {
                    // ignore
                }
            }

            Consumer.isPaused = false;
            ConfigHandler.migrationRunning = false;
        }
    }

    private static Connection getSourceConnection(String type) throws Exception {
        if (type.equals("sqlite")) {
            String database = "jdbc:sqlite:" + ConfigHandler.path + ConfigHandler.sqlite;
            return DriverManager.getConnection(database);
        }
        else if (type.equals("h2")) {
            Class.forName("org.h2.Driver");
            String database = "jdbc:h2:" + ConfigHandler.path + ConfigHandler.h2database;
            return DriverManager.getConnection(database);
        }
        return null;
    }

    private static Connection getTargetConnection(String type) throws Exception {
        if (type.equals("sqlite")) {
            String database = "jdbc:sqlite:" + ConfigHandler.path + ConfigHandler.sqlite;
            return DriverManager.getConnection(database);
        }
        else if (type.equals("h2")) {
            Class.forName("org.h2.Driver");
            String compressOption = Config.getGlobal().H2_COMPRESS ? ";COMPRESS=TRUE" : "";
            String database = "jdbc:h2:" + ConfigHandler.path + ConfigHandler.h2database + compressOption;
            return DriverManager.getConnection(database);
        }
        return null;
    }

    private static void createTargetTables(Connection connection, String targetType) throws Exception {
        if (targetType.equals("sqlite")) {
            Database.createDatabaseTables(ConfigHandler.prefix, false, connection, false, false, false);
        }
        else if (targetType.equals("h2")) {
            Database.createDatabaseTables(ConfigHandler.prefix, false, connection, false, true, false);
        }
    }

    private static long migrateTable(Connection source, Connection target, String tableName, String targetType, CommandSender player) throws Exception {
        long rowCount = 0;

        // First, count total rows for progress reporting
        Statement countStmt = source.createStatement();
        ResultSet countRs = countStmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
        long totalRowsInTable = 0;
        if (countRs.next()) {
            totalRowsInTable = countRs.getLong(1);
        }
        countRs.close();
        countStmt.close();

        // Get column information from source
        Statement sourceStmt = source.createStatement();
        ResultSet rs = sourceStmt.executeQuery("SELECT * FROM " + tableName);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Build column list (skip rowid for H2 as it uses IDENTITY)
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        int startColumn = 1;

        // For H2 target, skip rowid column if it exists (IDENTITY auto-generates)
        if (targetType.equals("h2")) {
            String firstColumn = metaData.getColumnName(1).toLowerCase();
            if (firstColumn.equals("rowid")) {
                startColumn = 2;
            }
        }

        for (int i = startColumn; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            if (columns.length() > 0) {
                columns.append(", ");
                placeholders.append(", ");
            }
            columns.append(columnName);
            placeholders.append("?");
        }

        // Prepare insert statement
        String insertQuery = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
        PreparedStatement insertStmt = target.prepareStatement(insertQuery);

        // Begin transaction for target
        target.setAutoCommit(false);

        int batchSize = 0;
        int batchLimit = 1000;
        int lastReportedPercent = 0;

        while (rs.next()) {
            int paramIndex = 1;
            for (int i = startColumn; i <= columnCount; i++) {
                Object value = rs.getObject(i);
                insertStmt.setObject(paramIndex++, value);
            }
            insertStmt.addBatch();
            rowCount++;
            batchSize++;

            if (batchSize >= batchLimit) {
                insertStmt.executeBatch();
                target.commit();
                batchSize = 0;
                
                // Report progress every 10% for large tables
                if (totalRowsInTable > 10000) {
                    int currentPercent = (int) ((rowCount * 100.0) / totalRowsInTable);
                    if (currentPercent >= lastReportedPercent + 10) {
                        lastReportedPercent = (currentPercent / 10) * 10;
                        String rowsFormatted = NumberFormat.getInstance().format(rowCount);
                        String totalFormatted = NumberFormat.getInstance().format(totalRowsInTable);
                        Chat.sendGlobalMessage(player, "  " + lastReportedPercent + "% (" + rowsFormatted + "/" + totalFormatted + " rows)");
                    }
                }
            }
        }

        // Execute remaining batch
        if (batchSize > 0) {
            insertStmt.executeBatch();
            target.commit();
        }

        target.setAutoCommit(true);
        rs.close();
        sourceStmt.close();
        insertStmt.close();

        return rowCount;
    }
}
