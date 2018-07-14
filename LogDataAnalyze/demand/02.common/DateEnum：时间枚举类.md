##### 时间枚举类

```
package com.congcong.common;

/**
 * 时间枚举类
 */
public enum  DateEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public final String typeName;

    DateEnum(String typeName){
        this.typeName=typeName;
    }

    public static DateEnum valueofName(String name){
        for (DateEnum dateEnum:values()){
            if (name.equals(dateEnum.typeName)){
                return dateEnum;
            }
        }
        return null;
    }

}

```

