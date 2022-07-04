package com.kn.ods.dev;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class LogsAnalyzer {

	String file = "< path to your log file >";

	public static void main(String[] args) {
		try {
			new LogsAnalyzer().analyze();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	HashMap <String, List<JSONObject>> allLogsByThreads = new HashMap<>();
	public void analyze () throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(file));

		String currentLine = reader.readLine();
		while (currentLine != null) {
			parseCurrentLineExtractingTheThreadName(currentLine);
			currentLine = reader.readLine();
		}

		// Now all the logs are in the allLogsByThreads by their "thread".
		for (String threadName : allLogsByThreads.keySet()) {
			calculateThreadDurations(threadName, allLogsByThreads.get(threadName));
		}

		// now the thread times are in the threadDurations
		List<Long> sortedDurations = threadDurations.keySet().stream().sorted().collect(Collectors.toList());
		for (Long duration: sortedDurations) {
			if (duration == 0L) {
				continue;
			}
			List<String> threadsByDuration = threadDurations.get(duration);

			for (String threadName : threadsByDuration) {
				System.out.println("duration " + duration + " thread name: " + threadName);
			}
		}

	}

	private void parseCurrentLineExtractingTheThreadName(String currentLine) {
		try {
			JSONObject currentLog = new JSONObject(currentLine);
			String threadName = currentLog.getString("thread");

			List<JSONObject> currentList = allLogsByThreads.computeIfAbsent(
					threadName,
					k -> new ArrayList<>()
			);
			currentList.add(currentLog);

		} catch (Exception e) {
			System.out.println("Unable to process currentLine = " + currentLine + " \n  "+ e);
		}
	}

	HashMap <Long, List<String>> threadDurations = new HashMap<>();
	private void calculateThreadDurations(String threadName, List<JSONObject> jsonObjects) {
		LocalDateTime firstLogTime = getTimeOfIndex(jsonObjects, 0);
		LocalDateTime lastLogTime = getTimeOfIndex(jsonObjects, jsonObjects.size()-1);

		long duration = ChronoUnit.SECONDS.between(firstLogTime, lastLogTime);

		List<String> threads = threadDurations.computeIfAbsent(duration, k -> new ArrayList<>());
		threads.add(threadName);
	}

	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
	private LocalDateTime getTimeOfIndex(List<JSONObject> jsonObjects, int i) {
		String timeStr = jsonObjects.get(i).getString("time");
		return LocalDateTime.parse(timeStr, formatter);
	}


}
