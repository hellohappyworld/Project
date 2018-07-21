#### CategorySortKey

```
package com.qf.sessionanalyze1705.spark.session;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 自定义排序：
 * 进行排序需要几个字段：点击次数，下单次数，支付次数
 * 需要实现Ordered接口中的几个方法进行排序
 * 需要与其它几个key进行比较，判断大于、大于等于、小于、小于等于
 * 必须要实现Serializable接口进行序列化
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {
    //点击次数
    private long clickCount;
    //下单次数
    private long orderCount;
    //支付次数
    private long payCount;

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public int compare(CategorySortKey that) {
        if (clickCount - that.getClickCount() != 0) {
            return (int) (clickCount - that.getClickCount());
        } else if (orderCount - that.getOrderCount() != 0) {
            return (int) (orderCount - that.getOrderCount());
        } else if (payCount - that.getPayCount() != 0) {
            return (int) (payCount - that.getPayCount());
        }

        return 0;
    }

    @Override
    public boolean $less(CategorySortKey that) {
        if (clickCount < that.getClickCount()) {
            return true;
        } else if (clickCount == that.getClickCount()
                && orderCount < that.getOrderCount()) {
            return true;
        } else if (clickCount == that.getClickCount()
                && orderCount == that.getOrderCount()
                && payCount < that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey that) {
        if (clickCount > that.getClickCount()) {
            return true;
        } else if (clickCount == that.getClickCount()
                && orderCount > that.getOrderCount()) {
            return true;
        } else if (clickCount == that.getClickCount()
                && orderCount == that.getOrderCount()
                && payCount > that.getPayCount()) {
            return true;
        }

        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey that) {
        if ($less(that)) {
            return true;
        } else if (clickCount == that.getClickCount()
                && orderCount == that.getOrderCount()
                && payCount == that.getPayCount()) {
            return true;
        }

        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey that) {
        if ($greater(that)) {
            return true;
        } else if (clickCount == that.getClickCount()
                && orderCount == that.getOrderCount()
                && payCount == that.getPayCount()) {
            return true;
        }

        return false;
    }

    @Override
    public int compareTo(CategorySortKey that) {
        return compare(that);
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }
}

```

