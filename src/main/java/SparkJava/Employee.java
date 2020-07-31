package SparkJava;

import scala.Int;

import java.io.Serializable;

public class Employee implements Serializable
{
    private Integer employeeId;
    private String employeeName;
    private Integer employeeSalary;
    private Integer departmentId;

    public Employee(Integer employeeId, String employeeName, Integer employeeSalary, Integer departmentId)
    {
        this.employeeId = employeeId;
        this.employeeName = employeeName;
        this.employeeSalary = employeeSalary;
        this.departmentId =  departmentId;
    }

    public void setDepartmentId(Integer departmentId) {
        this.departmentId = departmentId;
    }

    public Integer getDepartmentId() {
        return departmentId;
    }

    public void setEmployeeId(Integer employeeId) {
        this.employeeId = employeeId;
    }

    public Integer getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeName(String employeeName) {
        this.employeeName = employeeName;
    }

    public String getEmployeeName() {
        return employeeName;
    }

    public void setEmployeeSalary(Integer employeeSalary) {
        this.employeeSalary = employeeSalary;
    }

    public Integer getEmployeeSalary() {
        return employeeSalary;
    }

    public String toString()
    {
        return "Employee{" + "employeeId = '" + employeeId + "' , employeeName = '" + employeeName + "' , employeeSalary = '" + employeeSalary + "' , departmentid = '" + departmentId + "'}";
    }
}
