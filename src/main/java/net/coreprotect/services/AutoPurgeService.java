package net.coreprotect.services;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.database.Database;
import net.coreprotect.language.Phrase;
import net.coreprotect.utility.Chat;

/**
 * Service responsible for automatic data purging based on configuration
 */
public class AutoPurgeService extends Consumer {

    /**
     * Parse time string into seconds (e.g., "14d" -> 1209600)
     * 
     * @param timeString
     *            The time string to parse
     * @return Time in seconds, or 0 if disabled/invalid
     */
    public static long parseAutoPurgeTime(String timeString) {
        if (timeString == null || timeString.equals("0") || timeString.isEmpty()) {
            return 0;
        }

        timeString = timeString.toLowerCase(Locale.ROOT).trim();

        double y = 0, mo = 0, w = 0, d = 0, h = 0, m = 0, s = 0;

        // Handle months first to avoid conflict with minutes
        String processed = timeString.replaceAll("mo", "mo:");
        processed = processed.replaceAll("y", "y:");
        processed = processed.replaceAll("(?<!o)m(?!o)", "m:"); // Match 'm' not preceded by 'o' and not followed by 'o'
        processed = processed.replaceAll("w", "w:");
        processed = processed.replaceAll("d", "d:");
        processed = processed.replaceAll("h", "h:");
        processed = processed.replaceAll("s", "s:");

        String[] parts = processed.split(":");
        for (String part : parts) {
            if (part.isEmpty()) continue;

            if (part.endsWith("y")) {
                String num = part.replaceAll("[^0-9.]", "");
                if (!num.isEmpty()) y = Double.parseDouble(num);
            }
            else if (part.endsWith("mo")) {
                String num = part.replaceAll("[^0-9.]", "");
                if (!num.isEmpty()) mo = Double.parseDouble(num);
            }
            else if (part.endsWith("w")) {
                String num = part.replaceAll("[^0-9.]", "");
                if (!num.isEmpty()) w = Double.parseDouble(num);
            }
            else if (part.endsWith("d")) {
                String num = part.replaceAll("[^0-9.]", "");
                if (!num.isEmpty()) d = Double.parseDouble(num);
            }
            else if (part.endsWith("h")) {
                String num = part.replaceAll("[^0-9.]", "");
                if (!num.isEmpty()) h = Double.parseDouble(num);
            }
            else if (part.endsWith("m")) {
                String num = part.replaceAll("[^0-9.]", "");
                if (!num.isEmpty()) m = Double.parseDouble(num);
            }
            else if (part.endsWith("s")) {
                String num = part.replaceAll("[^0-9.]", "");
                if (!num.isEmpty()) s = Double.parseDouble(num);
            }
        }

        // Calculate total seconds
        // 1 year = 365 days = 31536000 seconds
        // 1 month = 30 days = 2592000 seconds
        return (long) ((y * 31536000) + (mo * 2592000) + (w * 604800) + (d * 86400) + (h * 3600) + (m * 60) + s);
    }

    /**
     * Check if auto-purge is enabled and run if necessary
     */
    public static void runAutoPurge() {
        String autoPurgeConfig = Config.getGlobal().AUTO_PURGE_DATA;
        long purgeTime = parseAutoPurgeTime(autoPurgeConfig);

        if (purgeTime <= 0) {
            return; // Auto-purge disabled
        }

        // Minimum 24 hours for console-triggered purge
        if (purgeTime < 86400) {
            Chat.console(Phrase.build(Phrase.AUTO_PURGE_MINIMUM));
            return;
        }

        Thread purgeThread = new Thread(() -> {
            try {
                // Wait a bit for server to fully start
                Thread.sleep(10000);

                // Don't run if other operations are in progress
                if (ConfigHandler.converterRunning || ConfigHandler.migrationRunning || ConfigHandler.purgeRunning) {
                    Chat.console(Phrase.build(Phrase.AUTO_PURGE_SKIPPED));
                    return;
                }

                performAutoPurge(purgeTime);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
        purgeThread.start();
    }

    /**
     * Perform the actual auto-purge operation
     * 
     * @param purgeTimeSeconds
     *            Time threshold in seconds - data older than this will be purged
     */
    private static void performAutoPurge(long purgeTimeSeconds) {
        try {
            long timestamp = System.currentTimeMillis() / 1000L;
            long timeThreshold = timestamp - purgeTimeSeconds;

            Chat.console(Phrase.build(Phrase.AUTO_PURGE_STARTED, formatTime(purgeTimeSeconds)));

            ConfigHandler.purgeRunning = true;

            // Wait for consumer to pause
            int waitCount = 0;
            while (!Consumer.pausedSuccess && waitCount < 30000) {
                Thread.sleep(1);
                waitCount++;
            }
            Consumer.isPaused = true;

            Connection connection = null;
            for (int i = 0; i <= 5; i++) {
                connection = Database.getConnection(false, 500);
                if (connection != null) {
                    break;
                }
                Thread.sleep(1000);
            }

            if (connection == null) {
                Chat.console(Phrase.build(Phrase.DATABASE_BUSY));
                Consumer.isPaused = false;
                ConfigHandler.purgeRunning = false;
                return;
            }

            long totalRemoved = 0;

            // Tables that have time-based data
            List<String> purgeTables = Arrays.asList("sign", "container", "item", "skull", "session", "chat", "command", "entity", "block");

            for (String table : purgeTables) {
                try {
                    String fullTableName = ConfigHandler.prefix + table;
                    String tableName = table.replaceAll("_", " ");
                    Chat.console(Phrase.build(Phrase.PURGE_PROCESSING, tableName));

                    // Count rows to be deleted
                    String countQuery = "SELECT COUNT(*) FROM " + fullTableName + " WHERE time < ?";
                    PreparedStatement countStmt = connection.prepareStatement(countQuery);
                    countStmt.setLong(1, timeThreshold);
                    var rs = countStmt.executeQuery();
                    long rowsToDelete = 0;
                    if (rs.next()) {
                        rowsToDelete = rs.getLong(1);
                    }
                    rs.close();
                    countStmt.close();

                    if (rowsToDelete > 0) {
                        // Delete old data
                        String deleteQuery = "DELETE FROM " + fullTableName + " WHERE time < ?";
                        PreparedStatement deleteStmt = connection.prepareStatement(deleteQuery);
                        deleteStmt.setLong(1, timeThreshold);
                        deleteStmt.executeUpdate();
                        deleteStmt.close();

                        totalRemoved += rowsToDelete;
                        String rowCount = NumberFormat.getInstance().format(rowsToDelete);
                        Chat.console(Phrase.build(Phrase.PURGE_ROWS, rowCount));
                    }
                }
                catch (Exception e) {
                    Chat.console(Phrase.build(Phrase.PURGE_ERROR, table));
                    e.printStackTrace();
                }
            }

            // Optimize database if significant data was removed
            if (totalRemoved > 1000) {
                Chat.console(Phrase.build(Phrase.PURGE_OPTIMIZING));

                if (!Config.getGlobal().MYSQL && !Config.getGlobal().H2) {
                    // SQLite VACUUM
                    try {
                        Statement statement = connection.createStatement();
                        statement.executeUpdate("VACUUM");
                        statement.close();
                    }
                    catch (Exception e) {
                        // Ignore vacuum errors
                    }
                }
            }

            connection.close();

            String totalCount = NumberFormat.getInstance().format(totalRemoved);
            Chat.console(Phrase.build(Phrase.AUTO_PURGE_SUCCESS, totalCount));

        }
        catch (Exception e) {
            Chat.console(Phrase.build(Phrase.PURGE_FAILED));
            e.printStackTrace();
        }
        finally {
            Consumer.isPaused = false;
            ConfigHandler.purgeRunning = false;
        }
    }

    /**
     * Format seconds into a human-readable time string
     */
    private static String formatTime(long seconds) {
        if (seconds >= 31536000) {
            long years = seconds / 31536000;
            return years + (years == 1 ? " year" : " years");
        }
        else if (seconds >= 2592000) {
            long months = seconds / 2592000;
            return months + (months == 1 ? " month" : " months");
        }
        else if (seconds >= 604800) {
            long weeks = seconds / 604800;
            return weeks + (weeks == 1 ? " week" : " weeks");
        }
        else if (seconds >= 86400) {
            long days = seconds / 86400;
            return days + (days == 1 ? " day" : " days");
        }
        else if (seconds >= 3600) {
            long hours = seconds / 3600;
            return hours + (hours == 1 ? " hour" : " hours");
        }
        else {
            long minutes = seconds / 60;
            return minutes + (minutes == 1 ? " minute" : " minutes");
        }
    }
}
