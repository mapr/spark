let loginUserInput = document.getElementById("loginUser");
let loginPasswordInput = document.getElementById("loginPassword");
let loginBtn = document.getElementById("loginBtn");
let ssoBtn = document.getElementById("ssoBtn");
let error = document.getElementById("error");
const uiBase = window.location.origin;

function signIn() {
    let userName = loginUserInput.value;
    let loginPassword = loginPasswordInput.value;
    const authCred = window.btoa(`${userName}:${loginPassword}`);
    const req = new Request(uiBase, {
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
            window.location.href = uiBase
        } catch (error) {
            console.error("Error:", error);
        }
    });
}

function ssoProceed() {
    fetch(uiBase + "?action=initSSO", {
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
    const req = new Request(uiBase, {});
    fetch(req).then((response) => {
        if (response.redirected && !response.url.startsWith(uiBase + "/login")) {
            window.location.href = response.url;
        }
    });
}

function logoutAction() {
    fetch(uiBase + "?action=logout", {})
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
    fetch(uiBase + "?action=ssoEnable", {}).then((response) => {
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
