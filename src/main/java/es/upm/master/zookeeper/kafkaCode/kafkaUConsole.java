package es.upm.master.zookeeper.kafkaCode;

import es.upm.master.zookeeper.kafkaCode.kafkaUConsole;
import es.upm.master.zookeeper.kafkaCode.ZKWriter;
import es.upm.master.zookeeper.kafkaCode.kafkaUConsole;
import org.apache.zookeeper.KeeperException;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class kafkaUConsole extends Thread {
    private JTextField kafkaUsername;
    private JButton kafkaRegister;
    private JPanel formulario;
    private JButton kafkaSend;
    private JComboBox kafkaComboBox;
    private JTextField kafkaTxtField;
    private JTextArea kafkaTextArea;
   // private JButton kafkaRead;
    private JTextArea kafkaChat;
    private JLabel kafkaMessageToLabel;
    private JLabel kafkaMessageLabel;
    private JLabel kafkaUsernameLabel;
    private JButton logOutButton;
    private JButton quitButton;
    private JButton kafkaReadButton;
    private JLabel chatLabel;
    private JScrollPane kafkaJScrollPane;
    public static String usernow;

    ZKWriter zkw = new ZKWriter();

    public kafkaUConsole() throws InterruptedException, IOException, KeeperException {


       // formulario.setVisible(true);
        kafkaUsernameLabel.setVisible(false);
        kafkaRegister.setVisible(true);
        kafkaSend.setVisible(false);
        kafkaComboBox.setVisible(false);
        kafkaTxtField.setVisible(false);
        kafkaTextArea.setVisible(false);
        //kafkaRead.setVisible(false);
        kafkaChat.setVisible(false);
        kafkaMessageToLabel.setVisible(false);
        kafkaMessageLabel.setVisible(false);
        quitButton.setVisible(true);
        logOutButton.setVisible(false);

        kafkaUsername.setVisible(true);
        kafkaUsernameLabel.setVisible(true);
        kafkaRegister.setVisible(true);
        kafkaSend.setVisible(false);
        kafkaComboBox.setVisible(false);
        kafkaTxtField.setVisible(false);
        kafkaTextArea.setVisible(false);
       // kafkaRead.setVisible(false);
        kafkaMessageToLabel.setVisible(false);
        kafkaMessageLabel.setVisible(false);
        logOutButton.setVisible(false);
        quitButton.setVisible(true);
      //  chatLabel.setVisible(false);
        kafkaReadButton.setVisible(false);
        kafkaJScrollPane.setVisible(false);


        kafkaRegister.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                String clientName = kafkaUsername.getText();

                formulario.setVisible(true);
                kafkaUsername.setEditable(false);
                kafkaUsernameLabel.setVisible(false);
                kafkaRegister.setVisible(false);
                kafkaSend.setVisible(true);
                kafkaComboBox.setVisible(true);
                kafkaTxtField.setVisible(false);
                kafkaTextArea.setVisible(true);
               // kafkaRead.setVisible(true);
                kafkaChat.setVisible(true);
                kafkaMessageToLabel.setVisible(true);
                kafkaMessageLabel.setVisible(true);
                logOutButton.setVisible(true);
                quitButton.setVisible(false);
            //    chatLabel.setVisible(true);
                kafkaReadButton.setVisible(true);
                kafkaJScrollPane.setVisible(true);




                try {
                    zkw.ZKWriter(clientName, kafkaUConsole.this);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

                try {
                    zkw.create();
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                try {
                    zkw.goOnline();
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                kafkaUConsole.usernow=kafkaUsername.getText();
                try {

                    new kafkaUConsole().start();
                    System.out.println("hello");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }


            }
        });



        kafkaSend.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {

                //String receiver = kafkaTxtField.getText();
                String receiver = kafkaComboBox.getSelectedItem().toString();
                String message = kafkaTextArea.getText();
                try {
                    zkw.send(receiver, message);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

            }
        });



        kafkaComboBox.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(FocusEvent focusEvent) {

                super.focusGained(focusEvent);
                kafkaComboBox.removeAllItems();

                List<String> childrenOnlineUsers = null;
                try {
                    childrenOnlineUsers = zkw.zoo.getChildren("/System/Online", false);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                kafkaComboBox.setModel(new DefaultComboBoxModel(childrenOnlineUsers.toArray()));


            }
        });


        logOutButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {

                try {
                    zkw.goOffline();
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                //formulario.setVisible(true);
                kafkaUsernameLabel.setVisible(true);
                kafkaRegister.setVisible(true);
                kafkaSend.setVisible(false);
                kafkaComboBox.setVisible(false);
                kafkaTxtField.setVisible(false);
                kafkaTextArea.setVisible(false);
               // kafkaRead.setVisible(false);
                kafkaChat.setVisible(false);
                kafkaMessageToLabel.setVisible(false);
                kafkaMessageLabel.setVisible(false);
                quitButton.setVisible(true);
                logOutButton.setVisible(false);
           //     chatLabel.setVisible(false);
                kafkaReadButton.setVisible(false);
                kafkaUsername.setEditable(true);
                kafkaJScrollPane.setVisible(false);


            }
        });
        quitButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {

                try {
                    zkw.zooDisconnect();
                    Thread.sleep(500);

                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                }

                System.exit(0);
            }
        });

/*
        kafkaReadButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                try {
                    zkw.read();
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (UnsupportedEncodingException e1) {
                    e1.printStackTrace();
                }

            }
        });*/


    }

    public void run(){
        Logger logger= Logger.getLogger("file");
        FileHandler fh;
       // if(usernow!=null)
        try {
            fh= new FileHandler("Log" + usernow+".log");
            zkw.ZKWriter(usernow, this);
            System.out.println("hey");
            logger.addHandler(fh);
            SimpleFormatter formatter =new SimpleFormatter();
            fh.setFormatter(formatter);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        while (true){
            try {

                List<String> mess=zkw.read();

                if (!mess.isEmpty()) {
                    System.out.println("messages in run"+mess);
                    // mini.addMessage(mess);
                    this.addMessage(mess, logger);
                }


            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    public void addMessage(List<String> messages,Logger logger){
        kafkaChat.setVisible(true);

        for (String received : messages) {

            logger.info(received);
            System.out.println("message singlemessage"+ received);

            //TODO: WHY MESSAGES ARE NOT APPEARING !!!!
            //kafkaChatTest.setText(received);
            // kafkaChat.append(received);
            //kafkaChat.setText("\n");
        }
    }




    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        JFrame frames = new JFrame("ZooApp");
        frames.setPreferredSize(new Dimension(500, 450));
        frames.setLocation(500, 350);
        frames.setContentPane(new kafkaUConsole().formulario);
        frames.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frames.pack();
        frames.setVisible(true);

    }

}
