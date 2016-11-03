source("S:/Institutional Research/Chen/R setup/ODBC Connection.R")
#for short name of college
COLLNM <- sqlFetch(SISInfo, 'COLLEGE')


PAG <- sqlQuery(MSUEDW, "select distinct Pid, GRADUATING_COHORT, COLLEGE_FIRST,  COLLEGE_DEGREE, 
                MAJOR_FIRST_SEMESTER, MAJOR_NAME_FIRST, MAJOR_DEGREE, MAJOR_NAME_DEGREE
                from OPB_PERS_FALL.PERSISTENCE_V
                where student_level='UN' and level_entry_status='FRST' and (ENTRANT_SUMMER_FALL='Y' or substr(ENTRY_TERM_CODE,1,1)='F')
                and  GRADUATING_COHORT in ('2015-2016','2014-2015','2013-2014','2012-2013','2011-2012')")

#use the short college name
library(sqldf)
PAG <- sqldf("select a.*, b.Short_Name as COLLEGE_FIRST_NAME, c.Short_Name as COLLEGE_DEGREE_NAME
             from PAG a 
             left join COLLNM b 
             on a.COLLEGE_FIRST=b.Coll_Code
             left join COLLNM c 
             on a.COLLEGE_DEGREE=c.Coll_Code")

#concate to prevent same character
PAG$COLLEGE_FIRST_NAME <- paste(PAG$COLLEGE_FIRST, PAG$COLLEGE_FIRST_NAME, sep = "-")
PAG$MAJOR_NAME_FIRST <- paste(PAG$MAJOR_FIRST_SEMESTER, PAG$MAJOR_NAME_FIRST, sep = "-")

PAG$COLLEGE_DEGREE_NAME <- paste(PAG$COLLEGE_DEGREE, PAG$COLLEGE_DEGREE_NAME, sep = "-")
PAG$MAJOR_NAME_DEGREE <- paste(PAG$MAJOR_DEGREE, PAG$MAJOR_NAME_DEGREE, sep = "-")

#aggregation
library(dplyr)

Agg2 <- PAG %>% group_by(GRADUATING_COHORT, COLLEGE_FIRST_NAME,COLLEGE_DEGREE_NAME, MAJOR_NAME_FIRST,  MAJOR_NAME_DEGREE) %>% summarise(count=n())



#gather all college and major from 2 time points

DS1<- sqldf("select distinct   COLLEGE_DEGREE_NAME as coll,  MAJOR_NAME_DEGREE as mjr
            from Agg2
            union
            select distinct COLLEGE_FIRST_NAME , MAJOR_NAME_FIRST
            from Agg2")

#for each college, build college following by majors within that college list
t<- split(DS1, DS1$coll)
listf <- function(x){ c(unique(x$coll),x$mjr)}
test <- sapply(t, listf)
listall <- as.data.frame(do.call(c,test))


colnames(listall) <- "Org"
listall$merge <-1

#main structure  for building the square matrix
DS2 <- sqldf("select a.org, b.org as org1
             from listall a, listall b
             on 1=1
             ")


lvl <- unique(DS2$org1)

levels(DS2$Org) <- lvl
#covert org1 from char to factor
DS2$org1 <- as.factor(DS2$org1)
levels(DS2$org1) <- lvl




#build loop around the degree college
degrcoll <- unique(PAG$COLLEGE_DEGREE)

#loop through each degree college
for (k in degrcoll){
        Agg1 <- PAG %>% filter(COLLEGE_DEGREE==k) %>%group_by(GRADUATING_COHORT, COLLEGE_FIRST_NAME,COLLEGE_DEGREE_NAME ) %>% summarise(count=n())
        
        Agg3 <- PAG %>% filter(COLLEGE_DEGREE==k) %>% group_by(GRADUATING_COHORT,  MAJOR_NAME_FIRST,  MAJOR_NAME_DEGREE) %>% summarise(count=n())
        Agg4 <- PAG %>% filter(COLLEGE_DEGREE==k) %>% group_by(GRADUATING_COHORT, COLLEGE_FIRST_NAME,  MAJOR_NAME_DEGREE) %>% summarise(count=n())
        Agg5 <- PAG %>% filter(COLLEGE_DEGREE==k) %>% group_by(GRADUATING_COHORT, MAJOR_NAME_FIRST,  COLLEGE_DEGREE_NAME) %>% summarise(count=n())
        
        
        names(Agg3)<-names(Agg1)
        names(Agg4) <- names(Agg1)
        names(Agg5) <- names(Agg1)
        mainds <- rbind(Agg1, Agg3, Agg4, Agg5)
        
        
        cohortseq <- unique(mainds$GRADUATING_COHORT)
        
        
        #for all choices, all five graduating cohort together
        DS2_2011_15 <- sqldf("select  a.org, a.org1, (case when b.count is null then 0 else b.count end) as count
                             from DS2 a
                             left join mainds b
                             on a.org=COLLEGE_DEGREE_NAME and a.org1=COLLEGE_FIRST_NAME  ")
        xtb_2011_15 <- xtabs(count ~ Org + org1,data=DS2_2011_15)
        
        
        
        #all five graduating cohorts from 2011-12 to 2015-16
        
        vec_2011_15<-vector()
        for(i in  seq(nrow(xtb_2011_15))) {
                x<-c(xtb_2011_15[nrow(xtb_2011_15)+1-i,])
                names(x)<-NULL
                vec_2011_15<-rbind(x,vec_2011_15)
                
        }
        
        
        #loop through each graduating cohort
        vec <- list("All"=vec_2011_15)
        
        for (j in cohortseq){
                mds_2009 <- mainds%>% filter(GRADUATING_COHORT==j)
                
                DS2_2009 <- sqldf("select  a.org, a.org1, (case when b.count is null then 0 else b.count end) as count
                                  from DS2 a
                                  left join mds_2009 b
                                  on  a.org=COLLEGE_DEGREE_NAME and a.org1=COLLEGE_FIRST_NAME  ")
                
                xtb_2009 <- xtabs(count ~ Org + org1,data=DS2_2009)
                vec_2009<-vector()
                for(i in  seq(nrow(xtb_2009))) {
                        x<-c(xtb_2009[nrow(xtb_2009)+1-i,])
                        names(x)<-NULL
                        vec_2009<-rbind(x,vec_2009)
                        
                        curlist <- list(vec_2009)
                        names(curlist) <- paste(substr(j,1,4), substr(j,8,9), sep = "-")
                }
               
                vec <- c(vec,curlist)
                
        }
        
        
        
        #region
        coll <- unique(DS1$coll)
        
        
        regionnum <- sapply(coll, function(x){ which(lvl== x)-1})
        names(regionnum) <- NULL
        
        
        names <- lvl
        
        
        
        list <- list("names"=names, "regions"=regionnum, "matrix"=vec)
        
        require(RJSONIO)
       
        
        jsonOut<-toJSON(list)
        #cat(jsonOut)
        
        sink(paste('data',k, '.json', collapse ='', sep=""))
        cat(jsonOut)
        
        sink()
        
        
        
        
        
        
        
}
        
        
        

       
        
        
        
      
        
       
        
      
        
      
    
        
       
      
        