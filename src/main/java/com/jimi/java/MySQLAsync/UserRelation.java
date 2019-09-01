package com.jimi.java.MySQLAsync;


public class UserRelation {

  private String imei;
  private String mcType;
  private String mcName;
  private long userId;
  private String bindUserId;
  private String currentUserParentId;
  private String validityUserParentId;
  private String groupId;
  private String appId;
  private java.sql.Timestamp platformExpirationTime;
  private String accAlarmStatus;
  private String enableFlag;
  private String staticTimeFlag;
  private java.sql.Timestamp createTime;
  private java.sql.Timestamp updateTime;
  private String groupAttentionId;
  private String groupOverdueId;
  private String groupNoBoundId;
  private String repayFlag;
  private long carOvnerId;

  public UserRelation UserRelation(String str){
    UserRelation u = new UserRelation();
    u.setImei(str);
    return u;
  }

  public String getImei() {
    return imei;
  }

  public void setImei(String imei) {
    this.imei = imei;
  }


  public String getMcType() {
    return mcType;
  }

  public void setMcType(String mcType) {
    this.mcType = mcType;
  }


  public String getMcName() {
    return mcName;
  }

  public void setMcName(String mcName) {
    this.mcName = mcName;
  }

  public long getUserId() {
    return userId;
  }

  public void setUserId(long userId) {
    this.userId = userId;
  }


  public String getBindUserId() {
    return bindUserId;
  }

  public void setBindUserId(String bindUserId) {
    this.bindUserId = bindUserId;
  }


  public String getCurrentUserParentId() {
    return currentUserParentId;
  }

  public void setCurrentUserParentId(String currentUserParentId) {
    this.currentUserParentId = currentUserParentId;
  }


  public String getValidityUserParentId() {
    return validityUserParentId;
  }

  public void setValidityUserParentId(String validityUserParentId) {
    this.validityUserParentId = validityUserParentId;
  }


  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }


  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }


  public java.sql.Timestamp getPlatformExpirationTime() {
    return platformExpirationTime;
  }

  public void setPlatformExpirationTime(java.sql.Timestamp platformExpirationTime) {
    this.platformExpirationTime = platformExpirationTime;
  }

  public String getAccAlarmStatus() {
    return accAlarmStatus;
  }

  public void setAccAlarmStatus(String accAlarmStatus) {
    this.accAlarmStatus = accAlarmStatus;
  }


  public String getEnableFlag() {
    return enableFlag;
  }

  public void setEnableFlag(String enableFlag) {
    this.enableFlag = enableFlag;
  }


  public String getStaticTimeFlag() {
    return staticTimeFlag;
  }

  public void setStaticTimeFlag(String staticTimeFlag) {
    this.staticTimeFlag = staticTimeFlag;
  }


  public java.sql.Timestamp getCreateTime() {
    return createTime;
  }

  public void setCreateTime(java.sql.Timestamp createTime) {
    this.createTime = createTime;
  }


  public java.sql.Timestamp getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(java.sql.Timestamp updateTime) {
    this.updateTime = updateTime;
  }


  public String getGroupAttentionId() {
    return groupAttentionId;
  }

  public void setGroupAttentionId(String groupAttentionId) {
    this.groupAttentionId = groupAttentionId;
  }


  public String getGroupOverdueId() {
    return groupOverdueId;
  }

  public void setGroupOverdueId(String groupOverdueId) {
    this.groupOverdueId = groupOverdueId;
  }


  public String getGroupNoBoundId() {
    return groupNoBoundId;
  }

  public void setGroupNoBoundId(String groupNoBoundId) {
    this.groupNoBoundId = groupNoBoundId;
  }


  public String getRepayFlag() {
    return repayFlag;
  }

  public void setRepayFlag(String repayFlag) {
    this.repayFlag = repayFlag;
  }


  public long getCarOvnerId() {
    return carOvnerId;
  }

  public void setCarOvnerId(long carOvnerId) {
    this.carOvnerId = carOvnerId;
  }

  @Override
  public String toString() {
    return this.getUserId()+" " + this.getImei();
  }
}
