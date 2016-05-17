WORKDIR=../
OBJDIR=$(WORKDIR)/obj
SRCDIR=${WORKDIR}/src
SRCS=$(shell find $(SRCDIR) -name '*.java')
OBJS=$(patsubst %.java,%.class, $(subst $(SRCDIR),$(OBJDIR),$(SRCS)))
#HADOOP_PATH=此处为hadoop安装路径
HADOOP_CLASSPATH=$(shell ${HADOOP_PATH}hadoop classpath)
TARGET=$(WORKDIR)/stat-bigdata.jar

all: $(OBJS)
	cd $(OBJDIR) && cp $(SRCDIR)/init.properties $(OBJDIR)/init.properties && jar cvf $(TARGET) init.properties com && cp $(TARGET) $(SRCDIR)/stat-bigdata.jar

$(OBJDIR)/%.class: $(SRCDIR)/%.java
	javac -encoding utf-8 -classpath $(HADOOP_CLASSPATH):./ $< -d ../obj

clean:
	rm $(OBJS)
