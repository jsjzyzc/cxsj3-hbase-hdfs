package com.szewec.controller;

import com.szewec.service.HBaseService;
import com.szewec.utils.RequestMessage;
import com.szewec.utils.ResponseUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/hbase")
@Slf4j
@Api(value = "Hbase操作")
public class HbaseController {
    @Autowired
    private HBaseService hBaseService;
    /**
     * 根据tableName，family 创建表
     * @return
     */
    @GetMapping(value = "/createTable")
    @ApiOperation(value = "创建表", notes = "创建表")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "tableName",value = "表名",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "family",value = "列族名",paramType = "query",dataType = "string[]", required = true)
    })
    public RequestMessage createTable(String tableName, String[] family){
        try{
            if (StringUtils.isNotBlank(tableName) && family != null && family.length != 0){
                hBaseService.createTable(tableName,family);
                return ResponseUtil.successResponse("表创建成功！");
            }else{
                return ResponseUtil.failedResponse("表创建失败！","无法创建表！");
            }
        }catch (Exception e){
            log.error("表创建异常！", e);
            return ResponseUtil.failedResponse("表创建异常！", e.getMessage());
        }
    }

    /**
     * 根据tableName删除表
     */
    @GetMapping(value = "/deleteTable")
    @ApiOperation(value = "删除表", notes = "删除表")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "tableName",value = "表名",paramType = "query",dataType = "string", required = true)
    })
    public Object deleteTable(String tableName){
        try{
            if (StringUtils.isNotBlank(tableName)){
                hBaseService.deleteTable(tableName);
                return ResponseUtil.successResponseDelete("表删除成功！");
            }else{
                return ResponseUtil.failedResponse("表删除失败！","无法删除表！");
            }
        }catch (Exception e){
            log.error("表删除异常！", e);
            return ResponseUtil.failedResponse("表删除异常！", e.getMessage());
        }
    }
    /**
     * 根据tableName，rowkey，family coluem 查询单个数据
     */
    @GetMapping(value = "/getValue")
    @ApiOperation(value = "查询单个数据", notes = "查询单个数据")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "tableName",value = "表名",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "familyName",value = "列族名",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "rowKey",value = "行键",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "column",value = "列限定符",paramType = "query",dataType = "string", required = true)
    })
    public Object getValue(String tableName, String rowKey, String familyName, String column){
        try{
            if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(rowKey) && StringUtils.isNotBlank(familyName) && StringUtils.isNotBlank(column)){
                return ResponseUtil.successObjectResponse(hBaseService.getValue(tableName,rowKey,familyName,column));
            }else{
                return ResponseUtil.failedResponse("查询数据失败！","查询失败！");
            }
        }catch (Exception e){
            log.error("查询存在异常！", e);
            return ResponseUtil.failedResponse("查询存在异常！", e.getMessage());
        }
    }

    /**
     * 根据起始行键和终止行键输出信息
     */
    @GetMapping(value = "/getValueByStartStopRowKey")
    @ApiOperation(value = "查询多个数据", notes = "查询多个数据")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "tableName",value = "表名",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "familyName",value = "列族名",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "startRow",value = "起始行键",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "stopRow",value = "终止行键",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "column",value = "列限定符",paramType = "query",dataType = "string", required = true)
    })
    public Object getValueByStartStopRowKey(String tableName, String familyName, String column, String startRow, String stopRow){
        try{
            if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(startRow) && StringUtils.isNotBlank(stopRow) && StringUtils.isNotBlank(familyName) && StringUtils.isNotBlank(column)){
                return ResponseUtil.successListResponse(hBaseService.getValueByStartStopRowKey(tableName,familyName,column,startRow,stopRow));
            }else{
                return ResponseUtil.failedResponse("查询数据失败！","查询失败！");
            }
        }catch (Exception e){
            log.error("查询存在异常！", e);
            return ResponseUtil.failedResponse("查询存在异常！", e.getMessage());
        }
    }

    /**
     * 根据tableName，rowkey，family coluem 插入单个数据
     */
    @GetMapping(value = "/insertRow")
    @ApiOperation(value = "插入单个数据", notes = "插入单个数据")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "tableName",value = "表名",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "familyName",value = "列族名",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "rowKey",value = "行键",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "column",value = "列限定符",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "value",value = "值",paramType = "query",dataType = "string", required = true)
    })
    public Object insertRow(String tableName, String rowKey,
                            String familyName, String column, String value){
        try{
            if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(familyName) && StringUtils.isNotBlank(rowKey) && StringUtils.isNotBlank(column) && StringUtils.isNotBlank(value)){
                hBaseService.insertRow(tableName,rowKey,familyName,column,value);
                return ResponseUtil.successResponsePosts("插入成功");
            }else{
                return ResponseUtil.failedResponse("插入失败！","无法插入！");
            }
        }catch (Exception e){
            log.error("查询存在异常！", e);
            return ResponseUtil.failedResponse("查询存在异常！", e.getMessage());
        }
    }

    /**
     * 根据tableName，rowkey，family coluem 删除单个数据
     */
    @GetMapping(value = "/deleteColumn")
    @ApiOperation(value = "删除单个数据", notes = "删除单个数据")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "tableName",value = "表名",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "familyName",value = "列族名",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "rowKey",value = "行键",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "column",value = "列限定符",paramType = "query",dataType = "string", required = true)
    })
    public Object deleteColumn(String tableName, String rowKey,
                               String familyName, String column){
        try{
            if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(familyName) && StringUtils.isNotBlank(rowKey) && StringUtils.isNotBlank(column) ){
                hBaseService.deleteColumn(tableName,rowKey,familyName,column);
                return ResponseUtil.successResponsePosts("删除成功");
            }else{
                return ResponseUtil.failedResponse("删除失败！","无法删除！");
            }
        }catch (Exception e){
            log.error("删除存在异常！", e);
            return ResponseUtil.failedResponse("删除存在异常！", e.getMessage());
        }
    }

    /**
     * 根据tableName，rowkey 删除整行数据
     */
    @GetMapping(value = "/deleteAllColumn")
    @ApiOperation(value = "删除整行数据", notes = "删除整行数据")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "tableName",value = "表名",paramType = "query",dataType = "string", required = true),
            @ApiImplicitParam(name = "rowKey",value = "行键",paramType = "query",dataType = "string", required = true)
    })
    public RequestMessage deleteAllColumn(String tableName, String rowKey){
        try{
            if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(rowKey)){
                hBaseService.deleteAllColumn(tableName,rowKey);
                return ResponseUtil.successResponse("删除整行数据成功！");
            }else{
                return ResponseUtil.failedResponse("删除整行数据失败！","无法删除！");
            }
        }catch (Exception e){
            log.error("删除异常！", e);
            return ResponseUtil.failedResponse("删除异常！", e.getMessage());
        }
    }
}
