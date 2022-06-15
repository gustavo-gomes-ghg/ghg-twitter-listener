
app.get("/api/rules", async (req, res) => {
    if (!BEARER_TOKEN) {
      res.status(400).send(authMessage);
    }
  
    const token = BEARER_TOKEN;
    const requestConfig = {
      url: rulesURL,
      auth: {
        bearer: token,
      },
      json: true,
    };
  
    try {
      const response = await get(requestConfig);
  
      if (response.statusCode !== 200) {
        if (response.statusCode === 403) {
          res.status(403).send(response.body);
        } else {
          throw new Error(response.body.error.message);
        }
      }
  
      res.send(response);
    } catch (e) {
      res.send(e);
    }
});
  
app.post("/api/rules", async (req, res) => {
    if (!BEARER_TOKEN) {
        res.status(400).send(authMessage);
    }

    const token = BEARER_TOKEN;
    const requestConfig = {
        url: rulesURL,
        auth: {
        bearer: token,
        },
        json: req.body,
    };

    try {
        const response = await post(requestConfig);

        if (response.statusCode === 200 || response.statusCode === 201) {
        res.send(response);
        } else {
        throw new Error(response);
        }
    } catch (e) {
        res.send(e);
    }
});