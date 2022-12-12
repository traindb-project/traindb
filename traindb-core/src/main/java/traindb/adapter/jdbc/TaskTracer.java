package traindb.adapter.jdbc;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import traindb.catalog.pm.MTask;

public class TaskTracer {
    private long lastNanoTime;
    int          idx;
    List<MTask>  taskStatus;

    public TaskTracer() {
        this.lastNanoTime = System.nanoTime();
        this.idx = 0;
        this.taskStatus = new ArrayList<MTask>();
    }

    public List<MTask> getTaskLog() { return taskStatus;    }

    public void startTaskTracer(String query) {
//        this.taskStatus.clear();
        this.idx = 0;

        LocalDateTime now = LocalDateTime.now();
        String taskTime = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));

        MTask newTask = new MTask(taskTime , this.idx, query, "FAIL");
        this.taskStatus.add(newTask);

        //printStatus( newTask, "startTaskTracer" );
    }

    public void endTaskTracer() {
        String taskStatus = "SUCCESS";

        MTask currTask = this.taskStatus.get(0);
        currTask.setStatus(taskStatus);

        //printStatus( currTask, "endTaskTracer" );
    }

    public void openTaskTime(String event) {
        this.idx++;
        this.lastNanoTime = System.nanoTime();
        String taskStatus = "START";

        MTask newTask = new MTask("" , this.idx, event, taskStatus);
        this.taskStatus.add(newTask);

        //printStatus( newTask, "open task" );
    }

    public void closeTaskTime(String event) {
        long newNanoTime = System.nanoTime();
        long elapsed = newNanoTime - this.lastNanoTime;
        String taskTime = getExecutionTime(elapsed);

        MTask currTask = this.taskStatus.get(this.idx);
        currTask.setTime(taskTime);
        currTask.setStatus(event);

        //printStatus( currTask, "end task" );
    }

    public static String getExecutionTime(long elapsed)
    {
        long time_ms = TimeUnit.NANOSECONDS.toMicros(elapsed);
        long time_sec = TimeUnit.NANOSECONDS.toSeconds(elapsed);
        long time_min = TimeUnit.NANOSECONDS.toMinutes(elapsed);
        long time_hour = TimeUnit.NANOSECONDS.toHours(elapsed);
        long time_days = TimeUnit.MILLISECONDS.toDays(elapsed);
        String res = "";

        if (time_days == 0) {
            if ( time_hour == 0 ) {
                if ( time_min == 0 )
                    res = String.format("                 %02d.%06d", time_sec, time_ms);
                else
                    res = String.format("              %02d:%02d.%06d", time_min, time_sec, time_ms);
            }
            else
                res = String.format("           %02d:%02d:%02d.%06d", time_hour, time_min, time_sec, time_ms);
        }
        else {
            res = String.format("        %dd %02d:%02d:%02d.%06d", time_days, time_hour, time_min, time_sec,time_ms);
        }
        return res;
    }

    public void printStatus(MTask currTask, String msg)
    {
        System.out.println("--------  " + msg + "   ----------");
        System.out.println("time   = " + currTask.getTime());
        System.out.println("idx    = " + currTask.getIdx());
        System.out.println("task   = " + currTask.getTask());
        System.out.println("status = " + currTask.getStatus());
        System.out.println();
    }
}
