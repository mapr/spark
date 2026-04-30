let loginUserInput = document.getElementById("loginUser");
let loginPasswordInput = document.getElementById("loginPassword");
let loginBtn = document.getElementById("loginBtn");
let ssoBtn = document.getElementById("ssoBtn");
let error = document.getElementById("error");

function getRequestedTargetPath() {
    const next = new URLSearchParams(window.location.search).get("next");
    if (!next) {
        return "/";
    }
    if (next.startsWith("/") && !next.startsWith("//")) {
        return next;
    }
    try {
        const parsed = new URL(next, window.location.origin);
        if (parsed.origin === window.location.origin) {
            return parsed.pathname + parsed.search + parsed.hash;
        }
    } catch (e) {
        console.warn("Ignoring invalid next parameter", e);
    }
    return "/";
}

function signIn() {
    let userName = loginUserInput.value;
    let loginPassword = loginPasswordInput.value;
    const requestedTargetPath = getRequestedTargetPath();
    const authCred = window.btoa(`${userName}:${loginPassword}`);
    const req = new Request(requestedTargetPath, {
        credentials: 'include',
        headers: {
            'Content-Type': 'text/plain',
            'Authorization': `Basic ${authCred}`,
            'Request-Type': "Authentication",
        },
    });
    fetch(req).then((response) => {
        try {
            if (!response.ok) {
                error.textContent = "Invalid username or password"
                error.style.color = "red"
                throw new Error(`Response status: ${response.status}`);
            }
            error.textContent = ""
            window.location.href = requestedTargetPath
        } catch (error) {
            console.error("Error:", error);
        }
    });
}

function ssoProceed() {
    fetch(window.location.origin + "?action=initSSO", {
        headers: {
            'Content-Type': 'application/json'
        },
    }).then((response) => {
        if (!response.ok) {
            throw new Error(`Response status: ${response.status}`);
        }
        response.json().then(data => {
            console.log(data.loginURL);
            window.location.href = data.loginURL;
        })
    });
}

function checkRedirect() {
    const requestedTargetPath = getRequestedTargetPath();
    const req = new Request(requestedTargetPath, {});
    fetch(req).then((response) => {
        if (response.redirected && !response.url.includes("/login")) {
            window.location.href = response.url;
        }
    });
}

function logoutAction() {
    fetch(window.location.origin + "?action=logout", {})
        .then((response) => {
            if (!response.ok) {
                throw new Error(`Response status: ${response.status}`);
            }
            window.location.href = response.headers.get('Location');
        })
        .catch(error => {
            console.error("Error during logout:", error);
        });
}

function checkCheckSsoButton() {
    fetch(window.location.origin + "?action=ssoEnable", {}).then((response) => {
        if (!response.ok) {
            ssoBtn.disabled = true;
        } else {
            ssoBtn.disabled = false;
        }
    });
}

loginBtn.addEventListener("click", function (e) {
    e.preventDefault();
    signIn();
});

loginPasswordInput.addEventListener("keydown", function (e) {
    if (e.key === "Enter") {
        e.preventDefault();
        signIn();
    }
});

loginUserInput.addEventListener("keydown", function (e) {
    if (e.key === "Enter") {
        e.preventDefault();
        signIn();
    }
});

ssoBtn.addEventListener("click", function (e) {
    e.preventDefault();
    ssoProceed();
});

window.addEventListener("DOMContentLoaded", (event) => {
    checkRedirect();
    checkCheckSsoButton();
});
