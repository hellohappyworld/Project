##### 平台维度

```
package com.congcong.anlystic.modle.dim.base;

import com.congcong.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 平台维度
 */
public class PlatformDimension extends BaseDimension {
    private int id;
    private String platformName;

    public PlatformDimension() {
    }

    public PlatformDimension(String platformName) {
        this.platformName = platformName;
    }

    public PlatformDimension(int id, String platformName) {
        this(platformName);
        this.id = id;
    }

    public static List<PlatformDimension> buildList(String platformName) {
        if (StringUtils.isEmpty(platformName)) {
            platformName = GlobalConstants.DEFAULT_VALUE;
        }
        List<PlatformDimension> li = new ArrayList<>();
        li.add(new PlatformDimension(platformName));
        li.add(new PlatformDimension(GlobalConstants.ALL_OF_VALUE));

        return li;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.platformName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.platformName = in.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        PlatformDimension other = (PlatformDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.platformName.compareTo(other.platformName);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PlatformDimension that = (PlatformDimension) o;

        if (id != that.id) return false;
        return platformName != null ? platformName.equals(that.platformName) : that.platformName == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (platformName != null ? platformName.hashCode() : 0);
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }
}

```

