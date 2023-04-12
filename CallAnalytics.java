package Call_ANa;

import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

public class CallAnalytics {
	public static void main(String args[]) throws SQLException {
	
		System.out.println(getHourWithHighestCallVolume());
		System.out.println(getHourWithLongestCalls());
		System.out.println(getDayOfWeekWithHighestCallVolume());
		System.out.println(getDayOfWeekWithLongestCall());
	}

    private static final String DB_URL = "jdbc:mysql://localhost:3306/database_db";
    private static final String USER = "root";
    private static final String PASS = "root";
    
    private static final String CALL_TABLE_NAME = "call_ana";
    private static final String CALL_ID_COLUMN = "id";
    private static final String CALL_FROM_NUMBER_COLUMN = "from_number";
    private static final String CALL_START_TIME_COLUMN = "start_time";
    private static final String CALL_END_TIME_COLUMN = "end_time";
    private static final String CALL_DURATION_COLUMN = "duration";
    
    
	
    
    // Method to get the hour of the day with the highest call volume
    public static int getHourWithHighestCallVolume() throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            Map<Integer, Integer> callVolumeByHour = new HashMap<>();
            String sql = "SELECT HOUR(start_time) AS hour, COUNT(*) AS call_volume "
                       + "FROM call_ana "
                       + " GROUP BY HOUR(start_time)";
            try (PreparedStatement stmt = conn.prepareStatement(sql);
                 ResultSet rs = stmt.executeQuery()) {
            	
                while (rs.next()) {
                    int hour = rs.getInt("hour");
                    int callVolume = rs.getInt("call_volume");
                    callVolumeByHour.put(hour, callVolume);
                }
                
            }
                catch (SQLException e) {
                	e.printStackTrace();
    				// TODO: handle exception
    			}
            return Collections.max(callVolumeByHour.entrySet(), Map.Entry.comparingByValue()).getKey();
            }
            
        }
    
    
    // Method to get the hour of the day with the longest calls
    public static int getHourWithLongestCalls() throws SQLException {
    	
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
        	
            Map<Integer, Long> callDurationByHour = new HashMap<>();
            
            String sql = "SELECT HOUR(start_time) AS hour, SUM(duration) AS total_duration "
                       + "FROM call_ana "
                       + " GROUP BY HOUR(start_time)";
            
            try (PreparedStatement stmt = conn.prepareStatement(sql);
                 ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    int hour = rs.getInt("hour");
                    long totalDuration = rs.getLong("total_duration");
                    callDurationByHour.put(hour, totalDuration);
                }
            }
            catch (SQLException e) {
            	e.printStackTrace();
				// TODO: handle exception
			}
            return Collections.max(callDurationByHour.entrySet(), Map.Entry.comparingByValue()).getKey();
            
            
        }
    }
    
    // Method to get the day of the week with the highest call volume
    public static int getDayOfWeekWithHighestCallVolume() throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            Map<Integer, Integer> callVolumeByDayOfWeek = new HashMap<>();
            String sql = "SELECT DAYOFWEEK(start_time) AS day_of_week, COUNT(*) AS call_volume "
                       + "FROM call_ana "
                       + " GROUP BY DAYOFWEEK(start_time)";
            try (PreparedStatement stmt = conn.prepareStatement(sql);
                 ResultSet rs = stmt.executeQuery()) {
            	
                while (rs.next()) {
                	
                    int dayOfWeek = rs.getInt("day_of_week");
                    int callVolume = rs.getInt("call_volume");
                    callVolumeByDayOfWeek.put(dayOfWeek, callVolume);
                }
            }
                catch (SQLException e) {
                	e.printStackTrace();
    				// TODO: handle exception
    			}
            
            return Collections.max(callVolumeByDayOfWeek.entrySet(), Map.Entry.comparingByValue()).getKey();
            }
            
        }
    
    
    
    // Method to get the day of the week with the longest calls
    
    public static int getDayOfWeekWithLongestCall() throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            Map<Integer, Integer> callDurationByDayOfWeek = new HashMap<>();
            String longest = "SELECT DAYOFWEEK(start_time) AS day_of_week, MAX(duration) AS max_duration "
            + "FROM calls "
            + "GROUP BY DAYOFWEEK(start_time)";

            try (PreparedStatement ps = conn.prepareStatement(longest);
            	 ResultSet rs = ps.executeQuery()){
            	 while (rs.next()) {
            		 int dayOfWeek = rs.getInt("day_of_week");
            		 int maxDuration = rs.getInt("max_duration");
            		 callDurationByDayOfWeek.put(dayOfWeek, maxDuration);
			     }
            }
            	 catch (SQLException e) {
                 	e.printStackTrace();
     				// TODO: handle exception
     			}
     	
     		
//				System.out.println(dayOfWeekName + " - " + maxDuration + " seconds");
                 return Collections.max(callDurationByDayOfWeek.entrySet(), Map.Entry.comparingByValue()).getKey();
            }
            
    }
	
		
}
		    
