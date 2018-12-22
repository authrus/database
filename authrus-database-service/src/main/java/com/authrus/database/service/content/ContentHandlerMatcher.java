package com.authrus.database.service.content;

import com.authrus.http.Request;
import com.authrus.http.Response;

public interface ContentHandlerMatcher {
   ContentHandler match(Request request, Response response);
}
