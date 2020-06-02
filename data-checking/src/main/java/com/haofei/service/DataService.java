package com.haofei.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class DataService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier("sxgfJdbcTemplate")
    private JdbcTemplate sxgfJdbc;

    public String getTableCount(String tableName) {
        int endTime = (int) (System.currentTimeMillis()/1000 - 30*60);
        int startTime = endTime - 60;
        String sql = "select count(1) from " + tableName + " where event_time between " + startTime +" and " + endTime;
        System.out.println(sql);
        String jdbcCount = jdbcTemplate.queryForObject(sql,String.class);
        String sxgfCount = sxgfJdbc.queryForObject(sql,String.class);
        return "JDBC : " + jdbcCount + ",SXGF : " + sxgfCount ;
    }

    public List<String> getRowData(String tableName) {
        int endTime = (int) (System.currentTimeMillis()/1000 - 30*60);
        int startTime = endTime - 60;
        String sql = "select * from " + tableName + " where event_time between " + startTime +" and " + endTime + " limit 10";
        System.out.println(sql);
        List<Map<String, Object>> jdbcMaps = jdbcTemplate.queryForList(sql);
        List<Map<String, Object>> sxgfMaps = sxgfJdbc.queryForList(sql);
        List<String> rowData = new ArrayList<>(20);
        int size = jdbcMaps.size() < sxgfMaps.size() ? jdbcMaps.size() : sxgfMaps.size() ;
        for (int i = 0; i < size; i++) {
            rowData.add(jdbcMaps.get(i).toString());
            rowData.add(sxgfMaps.get(i).toString());
        }
        return rowData ;
    }
}
