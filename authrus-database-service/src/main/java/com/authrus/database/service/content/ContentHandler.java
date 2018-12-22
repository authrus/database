package com.authrus.database.service.content;

import com.authrus.http.Request;
import com.authrus.http.Response;

public interface ContentHandler {
   void handle(Request request, Response response);
}
