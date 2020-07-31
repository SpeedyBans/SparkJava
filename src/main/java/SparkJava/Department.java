package SparkJava;

import java.io.Serializable;

public class Department implements Serializable {
    private Integer departmentId;
    private String departmentName;

    public Department(Integer departmentId, String departmentName) {
        this.departmentId = departmentId;
        this.departmentName = departmentName;
    }

    public Department() {}

    public Integer getDepartmentId() {
        return departmentId;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public void setDepartmentId(Integer departmentId) {
        this.departmentId = departmentId;
    }

    public void setDepartmentName(String departmentName) {
        this.departmentName = departmentName;
    }

    public String toString()
    {
        return "Department{"+"departmentID = '"+departmentId+"' , departmentName = '"+departmentName+"' }";
    }
}