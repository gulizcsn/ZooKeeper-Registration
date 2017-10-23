/*
package es.upm.master.zookeeper.simpleExample;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class WelcomeInterface extends javax.swing.JFrame {
    private static ZooKeeper zoo;
    public static String userName;
    public void initComponents(ZooKeeper zoo){
        // Creating instance of JFrame
        JFrame frame = new JFrame("Application User Interface");
        // Setting the width and height of frame
        frame.setSize(350, 200);

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JPanel panel = new JPanel();
        // adding panel to frame
        frame.add(panel);
        */
/* calling user defined method for adding components
         * to the panel.
         *//*

        placeComponents(panel,zoo);
        // Setting the frame visibility to true
        frame.setVisible(true);
    }

    private static void placeComponents(JPanel panel, ZooKeeper zoo) {

        panel.setLayout(null);
        // Creating JLabel
        JLabel userLabel = new JLabel("User");
        userLabel.setBounds(10,20,80,25);
        panel.add(userLabel);

        */
/* Creating text field where user is supposed to
         * enter user name.
         *//*


        JTextField userText = new JTextField(20);
        userText.setBounds(100,20,165,25);
        panel.add(userText);

        JLabel optionsLabel = new JLabel("What would you like to do?");
        optionsLabel.setBounds(60,70,200,25);
        panel.add(optionsLabel);

        // Creating enroll button
        JButton createButton = new JButton("enroll");
        createButton.setBounds(60, 100, 80, 25);
        panel.add(createButton);

        // Creating quit button
        JButton quitButton = new JButton("quit");
        quitButton.setBounds(60, 130, 80, 25);
        panel.add(quitButton);


        userText.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent ae){
                String userName = userText.getText();
                System.out.println(userName);
            }
        });


    //catch action from enroll- call create
        createButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                //if (userName) {
                    System.out.println("going to create");
                    ZKManager zkm = new ZKManager();
                    try {
                        zkm.create(userName, zoo);
                    } catch (KeeperException ex) {
                        ex.printStackTrace();
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                */
/*}else{
                    //we should create banner saying that the username must be filled
                }
*//*

            }
        });

    //catch action from quit- call quit in ZkManager
        quitButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            //if (userName!=null) {
                System.out.println("going to quit");
                ZKManager zkm = new ZKManager();
                try {
                    zkm.quit(userName, zoo);
                } catch (KeeperException ex) {
                    ex.printStackTrace();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
          */
/*  }else{
                //we should create banner saying that the username must be filled
            }*//*

            }
        });
    }
}*/
