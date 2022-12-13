/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package traindb.task;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import traindb.catalog.pm.MTask;

public class TaskTracer {
  int idx;
  List<MTask> taskStatus;
  private long lastNanoTime;

  public TaskTracer() {
    this.lastNanoTime = System.nanoTime();
    this.idx = 0;
    this.taskStatus = new ArrayList<MTask>();
  }

  public static String getExecutionTime(long elapsed) {
    long ms = TimeUnit.NANOSECONDS.toMicros(elapsed) % 1000000L;
    long sec = TimeUnit.NANOSECONDS.toSeconds(elapsed) % 60;
    long min = TimeUnit.NANOSECONDS.toMinutes(elapsed) % 60;
    long hour = TimeUnit.NANOSECONDS.toHours(elapsed) % 24;
    long days = TimeUnit.NANOSECONDS.toDays(elapsed);
    String res = "";

    if (days == 0) {
      if (hour == 0) {
        if (min == 0) {
          res = String.format("                 %02d.%06d", sec, ms);
        } else {
          res = String.format("              %02d:%02d.%06d", min, sec, ms);
        }
      } else {
        res = String.format("           %02d:%02d:%02d.%06d", hour, min, sec, ms);
      }
    } else {
      res = String.format("        %dd %02d:%02d:%02d.%06d", days, hour, min, sec, ms);
    }
    return res;
  }

  public List<MTask> getTaskLog() {
    return taskStatus;
  }

  public void startTaskTracer(String query) {
    //this.taskStatus.clear();
    this.idx = 0;

    LocalDateTime now = LocalDateTime.now();
    String taskTime = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));

    MTask newTask = new MTask(taskTime, this.idx, query, "FAIL");
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

    MTask newTask = new MTask("", this.idx, event, taskStatus);
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

  public void printStatus(MTask currTask, String msg) {
    System.out.println("--------  " + msg + "   ----------");
    System.out.println("time   = " + currTask.getTime());
    System.out.println("idx    = " + currTask.getIdx());
    System.out.println("task   = " + currTask.getTask());
    System.out.println("status = " + currTask.getStatus());
    System.out.println();
  }
}
